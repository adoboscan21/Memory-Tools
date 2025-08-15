package handler

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"memory-tools/internal/globalconst"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	stdjson "encoding/json"

	jsoniter "github.com/json-iterator/go"
)

// ./internal/handler/query_commands.go

func (h *ConnectionHandler) handleCollectionQuery(r io.Reader, conn net.Conn) {
	// La lectura del comando ahora usa el io.Reader genérico.
	collectionName, queryJSONBytes, err := protocol.ReadCollectionQueryCommand(r)
	if err != nil {
		slog.Error("Failed to read COLLECTION_QUERY command payload", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_QUERY command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	// La lógica de autorización y comprobación no cambia.
	if !h.hasPermission(collectionName, globalconst.PermissionRead) {
		slog.Warn("Unauthorized query attempt",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"remote_addr", conn.RemoteAddr().String(),
		)
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for query", collectionName), nil)
		return
	}

	// La lógica del pool de objetos Query no cambia.
	query := queryPool.Get().(*Query)
	defer func() {
		query.Reset()
		queryPool.Put(query)
	}()

	if err := jsoniter.Unmarshal(queryJSONBytes, query); err != nil {
		slog.Warn("Failed to unmarshal query JSON",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"error", err,
		)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid query JSON format", nil)
		return
	}

	slog.Debug("Processing collection query", "user", h.AuthenticatedUser, "collection", collectionName, "query", string(queryJSONBytes))

	// La llamada a la lógica de procesamiento y el manejo de la respuesta no cambian.
	results, err := h.processCollectionQuery(collectionName, query)
	if err != nil {
		slog.Error("Error processing collection query",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"error", err,
		)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to execute query: %v", err), nil)
		return
	}

	responseBytes, err := jsoniter.Marshal(results)
	if err != nil {
		slog.Error("Error marshalling query results",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"error", err,
		)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal query results", nil)
		return
	}

	if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Query executed on collection '%s'", collectionName), responseBytes); err != nil {
		slog.Error("Failed to write COLLECTION_QUERY response", "error", err, "remote_addr", conn.RemoteAddr().String())
	}
}

// processCollectionQuery executes a complex query on a collection.
func (h *ConnectionHandler) processCollectionQuery(collectionName string, query *Query) (any, error) {
	colStore := h.CollectionManager.GetCollection(collectionName)

	// A "simple query" has no complex operations; it just retrieves data.
	isSimpleQuery := len(query.Filter) == 0 && len(query.OrderBy) == 0 &&
		len(query.Aggregations) == 0 && len(query.GroupBy) == 0 &&
		query.Distinct == "" && len(query.Lookups) == 0 && len(query.Projection) == 0 && !query.Count

	if isSimpleQuery {
		slog.Debug("Executing simple query fast path with streaming", "collection", collectionName)

		// Pre-allocate slice with a reasonable capacity. It will grow if needed.
		capacity := 1024
		if query.Limit != nil && *query.Limit > 0 {
			capacity = *query.Limit
		}
		rawResults := make([]stdjson.RawMessage, 0, capacity)

		var processedCount int = 0
		limit := -1 // -1 signifies no limit
		if query.Limit != nil {
			limit = *query.Limit
		}

		// Use the new, efficient StreamAll method to avoid deep copies and GC pressure.
		colStore.StreamAll(func(key string, value []byte) bool {
			// Handle OFFSET: Skip items until the offset is reached
			if processedCount < query.Offset {
				processedCount++
				return true // Continue to the next item
			}

			// Add the item's raw JSON to our results
			rawResults = append(rawResults, value)

			// Handle LIMIT: Stop streaming once we have enough items
			if limit != -1 && len(rawResults) >= limit {
				return false // Stop streaming
			}

			return true // Continue streaming
		})

		slog.Info("Simple query fast path finished", "collection", collectionName, "results_count", len(rawResults))
		return rawResults, nil
	}

	// --- Original Logic for Complex Queries ---
	slog.Debug("Executing complex query path", "collection", collectionName)

	// --- HOT SEARCH (IN RAM) ---
	candidateKeys, usedIndex, remainingFilter := h.findCandidateKeysFromFilter(colStore, query.Filter)

	var itemsData map[string][]byte
	if usedIndex {
		slog.Debug("Query optimizer using index(es) for hot data", "collection", collectionName, "candidate_keys", len(candidateKeys))
		itemsData = colStore.GetMany(candidateKeys)
	} else {
		slog.Debug("Query optimizer NOT using index for hot data, falling back to full scan", "collection", collectionName)
		itemsData = colStore.GetAll() // The slow path still uses GetAll
		remainingFilter = query.Filter
	}

	hotResultsMap := make(map[string]map[string]any)
	for k, vBytes := range itemsData {
		var val map[string]any
		if err := jsoniter.Unmarshal(vBytes, &val); err != nil {
			continue
		}
		if h.matchFilter(val, remainingFilter) {
			hotResultsMap[k] = val
		}
	}
	slog.Info("Hot data query finished", "collection", collectionName, "found_matches", len(hotResultsMap))

	finalResults := make([]map[string]any, 0, len(hotResultsMap))
	for _, hotItem := range hotResultsMap {
		finalResults = append(finalResults, hotItem)
	}

	shouldSkipColdSearch := false
	if query.Limit != nil && len(finalResults) >= *query.Limit {
		slog.Debug("Skipping cold search: Limit met with hot data.", "collection", collectionName, "limit", *query.Limit, "hot_results", len(finalResults))
		shouldSkipColdSearch = true
	}

	if !shouldSkipColdSearch {
		// --- COLD SEARCH (ON DISK) ---
		slog.Debug("Executing query against cold data (Disk)...", "collection", collectionName)
		coldMatcher := func(item map[string]any) bool {
			if id, ok := item[globalconst.ID].(string); ok {
				if _, existsInHot := hotResultsMap[id]; existsInHot {
					return false
				}
			}
			return h.matchFilter(item, query.Filter)
		}
		coldResults, err := persistence.SearchColdData(collectionName, coldMatcher)
		if err != nil {
			return nil, fmt.Errorf("error searching cold data: %w", err)
		}
		slog.Info("Cold data query finished", "collection", collectionName, "found_matches", len(coldResults))

		// --- MERGE RESULTS ---
		if len(coldResults) > 0 {
			finalResults = append(finalResults, coldResults...)
		}
	}

	slog.Info("Total results before processing", "count", len(finalResults))

	if query.Distinct != "" {
		distinctValues := make(map[any]bool)
		var resultList []any
		for _, item := range finalResults {
			if val, ok := item[query.Distinct]; ok && val != nil {
				if _, seen := distinctValues[val]; !seen {
					distinctValues[val] = true
					resultList = append(resultList, val)
				}
			}
		}
		return resultList, nil
	}
	if query.Count && len(query.Aggregations) == 0 && len(query.GroupBy) == 0 {
		return map[string]int{globalconst.AggCount: len(finalResults)}, nil
	}
	if len(query.Aggregations) > 0 || len(query.GroupBy) > 0 {
		var itemsForAgg []struct {
			Key string
			Val map[string]any
		}
		for _, res := range finalResults {
			key, _ := res[globalconst.ID].(string)
			itemsForAgg = append(itemsForAgg, struct {
				Key string
				Val map[string]any
			}{Key: key, Val: res})
		}
		return h.performAggregations(itemsForAgg, query)
	}
	if len(query.OrderBy) > 0 {
		sort.Slice(finalResults, func(i, j int) bool {
			for _, ob := range query.OrderBy {
				valA, okA := finalResults[i][ob.Field]
				valB, okB := finalResults[j][ob.Field]
				if !okA && !okB {
					continue
				}
				if !okA {
					return true
				}
				if !okB {
					return false
				}
				cmp := compare(valA, valB)
				if cmp != 0 {
					if ob.Direction == globalconst.SortDesc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
	}

	offset := min(max(query.Offset, 0), len(finalResults))
	paginatedResults := finalResults[offset:]
	if query.Limit != nil && *query.Limit >= 0 {
		limit := *query.Limit
		if limit > len(paginatedResults) {
			limit = len(paginatedResults)
		}
		if limit == 0 {
			paginatedResults = []map[string]any{}
		} else {
			paginatedResults = paginatedResults[:limit]
		}
	}

	// Chained Lookups (JOIN Pipeline)
	if len(query.Lookups) > 0 {
		currentResults := paginatedResults
		for _, lookupSpec := range query.Lookups {
			nextResults := []map[string]any{}
			for _, doc := range currentResults {
				localValue, ok := getNestedValue(doc, lookupSpec.LocalField)
				if !ok {
					doc[lookupSpec.As] = nil
					nextResults = append(nextResults, doc)
					continue
				}

				joinQuery := Query{
					Filter: map[string]any{
						"field": lookupSpec.ForeignField,
						"op":    globalconst.OpEqual,
						"value": localValue,
					},
				}

				joinedData, err := h.processCollectionQuery(lookupSpec.FromCollection, &joinQuery)
				if err != nil {
					slog.Warn("Lookup sub-query failed", "error", err, "from", lookupSpec.FromCollection)
					doc[lookupSpec.As] = nil
				} else {
					if joinedSlice, isSlice := joinedData.([]map[string]any); isSlice && len(joinedSlice) == 1 {
						doc[lookupSpec.As] = joinedSlice[0]
					} else {
						doc[lookupSpec.As] = joinedData
					}
				}
				nextResults = append(nextResults, doc)
			}
			currentResults = nextResults
		}
		paginatedResults = currentResults
	}

	// Projection (SELECT specific fields)
	if len(query.Projection) > 0 {
		projectedResults := make([]map[string]any, 0, len(paginatedResults))
		for _, fullDoc := range paginatedResults {
			projectedDoc := make(map[string]any)
			for _, fieldPath := range query.Projection {
				if value, ok := getNestedValue(fullDoc, fieldPath); ok {
					setNestedValue(projectedDoc, fieldPath, value)
				}
			}
			projectedResults = append(projectedResults, projectedDoc)
		}
		return projectedResults, nil
	}

	return paginatedResults, nil
}

// findCandidateKeysFromFilter is the advanced query optimizer.
// It tries to use indexes for '=', 'in', range operators, and now supports 'OR' clauses.
func (h *ConnectionHandler) findCandidateKeysFromFilter(colStore store.DataStore, filter map[string]any) (keys []string, usedIndex bool, remainingFilter map[string]any) {
	if len(filter) == 0 {
		return nil, false, filter
	}

	// --- NEW: Logic for compound filters with "OR" ---
	if orConditions, ok := filter[globalconst.OpOr].([]any); ok {
		unionKeys := make(map[string]struct{})
		allConditionsAreIndexable := true

		for _, cond := range orConditions {
			condMap, isMap := cond.(map[string]any)
			if !isMap {
				// Invalid condition format, this forces a full scan.
				allConditionsAreIndexable = false
				break
			}

			// We recursively call this function on the sub-filter.
			// This allows nesting of AND inside OR, etc.
			subKeys, subIndexUsed, _ := h.findCandidateKeysFromFilter(colStore, condMap)

			if !subIndexUsed {
				// If any part of the OR cannot use an index, the entire OR must be a full scan.
				allConditionsAreIndexable = false
				break
			}

			// Add the keys from the successful index lookup to our union set.
			for _, key := range subKeys {
				unionKeys[key] = struct{}{}
			}
		}

		if allConditionsAreIndexable && len(orConditions) > 0 {
			// Success! All conditions in the OR clause were resolved using indexes.
			finalKeys := make([]string, 0, len(unionKeys))
			for k := range unionKeys {
				finalKeys = append(finalKeys, k)
			}
			slog.Debug("Query optimizer: using indexes for 'OR' clause", "found_keys", len(finalKeys))
			// The entire OR filter was resolved, so the remaining filter is empty.
			return finalKeys, true, make(map[string]any)
		}
		// If we fall through, it means at least one part of the OR couldn't use an index.
		// The function will proceed to the fallback and return `nil, false, filter`.
	}

	// --- LOGIC FOR COMPOUND FILTERS WITH "AND" ---
	if andConditions, ok := filter[globalconst.OpAnd].([]any); ok {
		keySets := [][]string{}
		nonIndexedConditions := []any{}

		for _, cond := range andConditions {
			condMap, isMap := cond.(map[string]any)
			if !isMap {
				nonIndexedConditions = append(nonIndexedConditions, cond)
				continue
			}

			// Recursively call to handle nested conditions and use existing logic.
			subKeys, subIndexUsed, subRemainingFilter := h.findCandidateKeysFromFilter(colStore, condMap)

			if subIndexUsed {
				keySets = append(keySets, subKeys)
				// If the sub-filter was not fully resolved, add the remainder to the non-indexed list.
				if len(subRemainingFilter) > 0 {
					nonIndexedConditions = append(nonIndexedConditions, subRemainingFilter)
				}
			} else {
				nonIndexedConditions = append(nonIndexedConditions, condMap)
			}
		}

		if len(keySets) == 0 {
			// No index was used for any of the 'and' conditions.
			return nil, false, filter
		}

		// Intersect the results of all conditions that used an index.
		candidateKeys := intersectKeys(keySets)

		// Return a new filter containing only the conditions that did not use an index.
		newFilter := make(map[string]any)
		if len(nonIndexedConditions) > 0 {
			newFilter[globalconst.OpAnd] = nonIndexedConditions
		}

		return candidateKeys, true, newFilter
	}

	// --- LOGIC FOR SIMPLE FILTERS (NOT WRAPPED IN "AND" or "OR") ---
	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"]

	if fieldOk && opOk && colStore.HasIndex(field) {
		var keys []string
		var used bool

		switch op {
		case globalconst.OpEqual:
			keys, used = colStore.Lookup(field, value)
		case globalconst.OpIn:
			if values, isSlice := value.([]any); isSlice {
				unionKeys := make(map[string]struct{})
				for _, v := range values {
					lookupKeys, _ := colStore.Lookup(field, v)
					for _, key := range lookupKeys {
						unionKeys[key] = struct{}{}
					}
				}
				finalKeys := make([]string, 0, len(unionKeys))
				for k := range unionKeys {
					finalKeys = append(finalKeys, k)
				}
				keys = finalKeys
				used = true
			}
		case globalconst.OpGreaterThan:
			keys, used = colStore.LookupRange(field, value, nil, false, false)
		case globalconst.OpGreaterThanOrEqual:
			keys, used = colStore.LookupRange(field, value, nil, true, false)
		case globalconst.OpLessThan:
			keys, used = colStore.LookupRange(field, nil, value, false, false)
		case globalconst.OpLessThanOrEqual:
			keys, used = colStore.LookupRange(field, nil, value, false, true)
		case globalconst.OpBetween:
			if bounds, ok := value.([]any); ok && len(bounds) == 2 {
				keys, used = colStore.LookupRange(field, bounds[0], bounds[1], true, true)
			}
		}

		if used {
			slog.Debug("Query optimizer: using index for simple filter", "field", field, "op", op, "found_keys", len(keys))
			// Since this was the only condition, the remaining filter is empty.
			return keys, true, make(map[string]any)
		}
	}

	// If no index could be used, return the original filter for a full scan.
	return nil, false, filter
}

// matchFilter evaluates an item against a filter condition.
func (h *ConnectionHandler) matchFilter(item map[string]any, filter map[string]any) bool {
	if len(filter) == 0 {
		return true
	}

	if andConditions, ok := filter[globalconst.OpAnd].([]any); ok {
		for _, cond := range andConditions {
			if condMap, isMap := cond.(map[string]any); isMap {
				if !h.matchFilter(item, condMap) {
					return false
				}
			} else {
				slog.Warn("Invalid 'and' condition format in query filter", "condition", cond)
				return false
			}
		}
		return true
	}

	if orConditions, ok := filter[globalconst.OpOr].([]any); ok {
		for _, cond := range orConditions {
			if condMap, isMap := cond.(map[string]any); isMap {
				if h.matchFilter(item, condMap) {
					return true
				}
			} else {
				slog.Warn("Invalid 'or' condition format in query filter", "condition", cond)
				return false
			}
		}
		return false
	}

	if notCondition, ok := filter[globalconst.OpNot].(map[string]any); ok {
		return !h.matchFilter(item, notCondition)
	}

	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"]

	if !fieldOk || !opOk {
		slog.Warn("Invalid filter condition (missing field/op)", "filter", filter)
		return false
	}

	itemValue, itemValueExists := item[field]

	switch op {
	case globalconst.OpEqual:
		return itemValueExists && compare(itemValue, value) == 0
	case globalconst.OpNotEqual:
		return !itemValueExists || compare(itemValue, value) != 0
	case globalconst.OpGreaterThan:
		return itemValueExists && compare(itemValue, value) > 0
	case globalconst.OpGreaterThanOrEqual:
		return itemValueExists && compare(itemValue, value) >= 0
	case globalconst.OpLessThan:
		return itemValueExists && compare(itemValue, value) < 0
	case globalconst.OpLessThanOrEqual:
		return itemValueExists && compare(itemValue, value) <= 0
	case globalconst.OpLike:
		if !itemValueExists {
			return false
		}
		if sVal, isStr := itemValue.(string); isStr {
			if pattern, isStrPattern := value.(string); isStrPattern {
				// Basic LIKE to regex conversion: % -> .*
				// We also quote meta characters to prevent regex injection.
				pattern = strings.ReplaceAll(regexp.QuoteMeta(pattern), "%", ".*")
				matched, err := regexp.MatchString("(?i)^"+pattern+"$", sVal) // (?i) for case-insensitivity
				if err != nil {
					slog.Warn("Error in LIKE regex pattern", "pattern", pattern, "error", err)
					return false
				}
				return matched
			}
		}
		return false
	case globalconst.OpBetween:
		if !itemValueExists {
			return false
		}
		if values, ok := value.([]any); ok && len(values) == 2 {
			return compare(itemValue, values[0]) >= 0 && compare(itemValue, values[1]) <= 0
		}
		return false
	case globalconst.OpIn:
		if !itemValueExists {
			return false
		}
		if values, ok := value.([]any); ok {
			for _, v := range values {
				if compare(itemValue, v) == 0 {
					return true
				}
			}
		}
		return false
	case globalconst.OpIsNull:
		return !itemValueExists || itemValue == nil
	case globalconst.OpIsNotNull:
		return itemValueExists && itemValue != nil
	default:
		slog.Warn("Unsupported filter operator", "operator", op)
		return false
	}
}

// compare two any values. Returns -1 if a<b, 0 if a==b, 1 if a>b.
func compare(a, b any) int {
	if numA, okA := toFloat64(a); okA {
		if numB, okB := toFloat64(b); okB {
			if numA < numB {
				return -1
			}
			if numA > numB {
				return 1
			}
			return 0
		}
	}

	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)
	return strings.Compare(strA, strB)
}

// toFloat64 attempts to convert an any to float64, returns false if not a number.
func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case jsoniter.Number:
		f, err := v.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// performAggregations handles GROUP BY and aggregation functions.
func (h *ConnectionHandler) performAggregations(items []struct {
	Key string
	Val map[string]any
}, query *Query) (any, error) {
	groupedData := make(map[string][]map[string]any)

	if len(query.GroupBy) == 0 {
		groupKey := "_no_group_"
		groupedData[groupKey] = make([]map[string]any, 0, len(items))
		for _, item := range items {
			groupedData[groupKey] = append(groupedData[groupKey], item.Val)
		}
	} else {
		for _, item := range items {
			groupKeyParts := make([]string, len(query.GroupBy))
			for i, field := range query.GroupBy {
				if val, ok := item.Val[field]; ok && val != nil {
					groupKeyParts[i] = fmt.Sprintf("%v", val)
				} else {
					groupKeyParts[i] = "NULL"
				}
			}
			groupKey := strings.Join(groupKeyParts, "|")
			groupedData[groupKey] = append(groupedData[groupKey], item.Val)
		}
	}

	var aggregatedResults []map[string]any
	for groupKey, groupItems := range groupedData {
		resultRow := make(map[string]any)

		if len(query.GroupBy) > 0 {
			if groupKey != "_no_group_" {
				groupKeyValues := strings.Split(groupKey, "|")
				for i, field := range query.GroupBy {
					if i < len(groupKeyValues) {
						resultRow[field] = groupKeyValues[i]
					}
				}
			}
		}

		for aggName, agg := range query.Aggregations {
			var aggValue any
			var err error

			switch agg.Func {
			case globalconst.AggCount:
				if agg.Field == "*" {
					aggValue = len(groupItems)
				} else {
					count := 0
					for _, item := range groupItems {
						if _, ok := item[agg.Field]; ok {
							count++
						}
					}
					aggValue = count
				}
			case globalconst.AggSum, globalconst.AggAvg, globalconst.AggMin, globalconst.AggMax:
				numbers := []float64{}
				for _, item := range groupItems {
					if val, ok := item[agg.Field]; ok {
						if num, convertedOk := toFloat64(val); convertedOk {
							numbers = append(numbers, num)
						}
					}
				}

				if len(numbers) == 0 {
					aggValue = nil
					continue
				}

				switch agg.Func {
				case globalconst.AggSum:
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum
				case globalconst.AggAvg:
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum / float64(len(numbers))
				case globalconst.AggMin:
					min := numbers[0]
					for _, n := range numbers {
						if n < min {
							min = n
						}
					}
					aggValue = min
				case globalconst.AggMax:
					max := numbers[0]
					for _, n := range numbers {
						if n > max {
							max = n
						}
					}
					aggValue = max
				default:
					err = fmt.Errorf("unsupported aggregation function: %s", agg.Func)
				}
			default:
				err = fmt.Errorf("unsupported aggregation function: %s", agg.Func)
			}

			if err != nil {
				return nil, err
			}
			resultRow[aggName] = aggValue
		}

		if h.matchFilter(resultRow, query.Having) {
			aggregatedResults = append(aggregatedResults, resultRow)
		}
	}

	return aggregatedResults, nil
}

func min(a, b int) int {
	return int(math.Min(float64(a), float64(b)))
}

func max(a, b int) int {
	return int(math.Max(float64(a), float64(b)))
}

// intersectKeys calculates the intersection of multiple string slices (keys).
// It uses a map for optimal efficiency.
func intersectKeys(keySets [][]string) []string {
	if len(keySets) == 0 {
		return []string{}
	}

	// Use a map for the first key set for O(1) lookups.
	// To optimize, you could find the smallest key set and start with that.
	intersectionMap := make(map[string]struct{})
	for _, key := range keySets[0] {
		intersectionMap[key] = struct{}{}
	}

	// Iterate over the other key sets, keeping only the keys present in the map.
	for i := 1; i < len(keySets); i++ {
		currentSetMap := make(map[string]struct{})
		for _, key := range keySets[i] {
			// If the key exists in our current intersection, we keep it.
			if _, found := intersectionMap[key]; found {
				currentSetMap[key] = struct{}{}
			}
		}
		// The new intersection is the result of this step.
		intersectionMap = currentSetMap
	}

	// Convert the final map back to a slice.
	finalKeys := make([]string, 0, len(intersectionMap))
	for key := range intersectionMap {
		finalKeys = append(finalKeys, key)
	}

	return finalKeys
}

func getNestedValue(data map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var current any = data

	for _, part := range parts {
		// If the current level is a slice with one element, unwrap it automatically.
		if currentSlice, ok := current.([]any); ok && len(currentSlice) == 1 {
			current = currentSlice[0]
		} else if currentMapSlice, ok := current.([]map[string]any); ok && len(currentMapSlice) == 1 {
			current = currentMapSlice[0]
		}

		currentMap, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}

		value, found := currentMap[part]
		if !found {
			return nil, false
		}
		current = value
	}

	return current, true
}

// VERSIÓN CORREGIDA Y SIMPLIFICADA
func setNestedValue(data map[string]any, path string, value any) {
	parts := strings.Split(path, ".")
	currentMap := data

	for i, key := range parts {
		if i == len(parts)-1 {
			currentMap[key] = value
			return
		}

		// Si el siguiente nivel no existe, créalo.
		if _, ok := currentMap[key]; !ok {
			currentMap[key] = make(map[string]any)
		}

		// Avanza al siguiente nivel.
		nextMap, ok := currentMap[key].(map[string]any)
		if !ok {
			// Hay un conflicto: ya existe un valor que no es un mapa. No se puede continuar.
			return
		}
		currentMap = nextMap
	}
}
