package handler

import (
	"fmt"
	"log/slog"
	"math"
	"memory-tools/internal/persistence"
	"memory-tools/internal/protocol"
	"memory-tools/internal/store"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

// handleCollectionQuery processes the CmdCollectionQuery command.
func (h *ConnectionHandler) handleCollectionQuery(conn net.Conn) {
	collectionName, queryJSONBytes, err := protocol.ReadCollectionQueryCommand(conn)
	if err != nil {
		slog.Error("Failed to read COLLECTION_QUERY command", "error", err, "remote_addr", conn.RemoteAddr().String())
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_QUERY command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	// Authorization check
	if !h.hasPermission(collectionName, "read") {
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

	var query Query
	if err := json.Unmarshal(queryJSONBytes, &query); err != nil {
		slog.Warn("Failed to unmarshal query JSON",
			"user", h.AuthenticatedUser,
			"collection", collectionName,
			"error", err,
		)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid query JSON format", nil)
		return
	}

	slog.Debug("Processing collection query", "user", h.AuthenticatedUser, "collection", collectionName, "query", string(queryJSONBytes))
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

	responseBytes, err := json.Marshal(results)
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
func (h *ConnectionHandler) processCollectionQuery(collectionName string, query Query) (any, error) {
	colStore := h.CollectionManager.GetCollection(collectionName)

	// --- HOT SEARCH (IN RAM) ---
	slog.Debug("Executing query against hot data (RAM)...", "collection", collectionName)
	// The index optimization and in-memory filtering logic is maintained.
	candidateKeys, usedIndex, remainingFilter := h.findCandidateKeysFromFilter(colStore, query.Filter)

	var itemsData map[string][]byte
	if usedIndex {
		slog.Debug("Query optimizer using index(es) for hot data", "collection", collectionName, "candidate_keys", len(candidateKeys))
		itemsData = colStore.GetMany(candidateKeys)
	} else {
		slog.Debug("Query optimizer NOT using index for hot data, falling back to full scan", "collection", collectionName)
		itemsData = colStore.GetAll()
		remainingFilter = query.Filter
	}

	hotResultsMap := make(map[string]map[string]any) // Use a map to prevent duplicates

	for k, vBytes := range itemsData {
		var val map[string]any
		if err := json.Unmarshal(vBytes, &val); err != nil {
			continue
		}
		if h.matchFilter(val, remainingFilter) {
			hotResultsMap[k] = val // Store the hot result
		}
	}
	slog.Info("Hot data query finished", "collection", collectionName, "found_matches", len(hotResultsMap))

	// --- COLD SEARCH (ON DISK) ---
	slog.Debug("Executing query against cold data (Disk)...", "collection", collectionName)
	// We create a `matcher` function that reuses the `matchFilter` logic.
	// This is key to avoid repeating code.
	coldMatcher := func(item map[string]any) bool {
		// A cold item is only considered if it's not already in RAM (in hotResultsMap).
		if id, ok := item["_id"].(string); ok {
			if _, existsInHot := hotResultsMap[id]; existsInHot {
				return false // We already found it in RAM, don't add it again.
			}
		}
		// Apply the full, original filter to the cold data.
		return h.matchFilter(item, query.Filter)
	}

	// Invoke the new search function on the persistence layer.
	coldResults, err := persistence.SearchColdData(collectionName, coldMatcher)
	if err != nil {
		return nil, fmt.Errorf("error searching cold data: %w", err)
	}
	slog.Info("Cold data query finished", "collection", collectionName, "found_matches", len(coldResults))

	// --- MERGE RESULTS ---
	finalResults := make([]map[string]any, 0, len(hotResultsMap)+len(coldResults))
	for _, hotItem := range hotResultsMap {
		finalResults = append(finalResults, hotItem)
	}
	finalResults = append(finalResults, coldResults...)
	slog.Info("Hot and Cold results merged", "total_results_before_processing", len(finalResults))

	// From here on, the logic for `distinct`, `count`, `aggregations`, `order by`, and `limit/offset`
	// operates on `finalResults`, which contains the union of hot and cold data.
	// The code is the same as before, but it now operates on the `finalResults` slice.

	if query.Distinct != "" {
		// ... (distinct logic on `finalResults`) ...
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
		return map[string]int{"count": len(finalResults)}, nil
	}

	if len(query.Aggregations) > 0 || len(query.GroupBy) > 0 {
		// For aggregations, we need to convert `finalResults` to the expected format.
		var itemsForAgg []struct {
			Key string
			Val map[string]any
		}
		for _, res := range finalResults {
			key, _ := res["_id"].(string)
			itemsForAgg = append(itemsForAgg, struct {
				Key string
				Val map[string]any
			}{Key: key, Val: res})
		}
		return h.performAggregations(itemsForAgg, query)
	}

	// 3. Ordering (ORDER BY clause)
	if len(query.OrderBy) > 0 {
		sort.Slice(finalResults, func(i, j int) bool {
			// ... (sorting logic on `finalResults`, no changes needed) ...
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
					if ob.Direction == "desc" {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
	}

	// 4. Pagination (OFFSET and LIMIT)
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

	return paginatedResults, nil
}

// findCandidateKeysFromFilter is the advanced query optimizer.
// It tries to use indexes for '=', 'in', range operators, and now supports 'OR' clauses.
func (h *ConnectionHandler) findCandidateKeysFromFilter(colStore store.DataStore, filter map[string]any) (keys []string, usedIndex bool, remainingFilter map[string]any) {
	if len(filter) == 0 {
		return nil, false, filter
	}

	// --- NEW: Logic for compound filters with "OR" ---
	if orConditions, ok := filter["or"].([]any); ok {
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
	if andConditions, ok := filter["and"].([]any); ok {
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
			newFilter["and"] = nonIndexedConditions
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
		case "=":
			keys, used = colStore.Lookup(field, value)
		case "in":
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
		case ">":
			keys, used = colStore.LookupRange(field, value, nil, false, false)
		case ">=":
			keys, used = colStore.LookupRange(field, value, nil, true, false)
		case "<":
			keys, used = colStore.LookupRange(field, nil, value, false, false)
		case "<=":
			keys, used = colStore.LookupRange(field, nil, value, false, true)
		case "between":
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

	if andConditions, ok := filter["and"].([]any); ok {
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

	if orConditions, ok := filter["or"].([]any); ok {
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

	if notCondition, ok := filter["not"].(map[string]any); ok {
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
	case "=":
		return itemValueExists && compare(itemValue, value) == 0
	case "!=":
		return !itemValueExists || compare(itemValue, value) != 0
	case ">":
		return itemValueExists && compare(itemValue, value) > 0
	case ">=":
		return itemValueExists && compare(itemValue, value) >= 0
	case "<":
		return itemValueExists && compare(itemValue, value) < 0
	case "<=":
		return itemValueExists && compare(itemValue, value) <= 0
	case "like":
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
	case "between":
		if !itemValueExists {
			return false
		}
		if values, ok := value.([]any); ok && len(values) == 2 {
			return compare(itemValue, values[0]) >= 0 && compare(itemValue, values[1]) <= 0
		}
		return false
	case "in":
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
	case "is null":
		return !itemValueExists || itemValue == nil
	case "is not null":
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
}, query Query) (any, error) {
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
			case "count":
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
			case "sum", "avg", "min", "max":
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
				case "sum":
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum
				case "avg":
					sum := 0.0
					for _, n := range numbers {
						sum += n
					}
					aggValue = sum / float64(len(numbers))
				case "min":
					min := numbers[0]
					for _, n := range numbers {
						if n < min {
							min = n
						}
					}
					aggValue = min
				case "max":
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
