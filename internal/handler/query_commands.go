package handler

import (
	"fmt"
	"log"
	"math"
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
		log.Printf("Error reading COLLECTION_QUERY command from %s: %v", conn.RemoteAddr(), err)
		protocol.WriteResponse(conn, protocol.StatusBadCommand, "Invalid COLLECTION_QUERY command format", nil)
		return
	}
	if collectionName == "" {
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Collection name cannot be empty", nil)
		return
	}

	// Authorization check
	if !h.hasPermission(collectionName, "read") {
		protocol.WriteResponse(conn, protocol.StatusUnauthorized, fmt.Sprintf("UNAUTHORIZED: You do not have read permission for collection '%s'", collectionName), nil)
		return
	}

	if !h.CollectionManager.CollectionExists(collectionName) {
		protocol.WriteResponse(conn, protocol.StatusNotFound, fmt.Sprintf("NOT FOUND: Collection '%s' does not exist for query", collectionName), nil)
		return
	}

	var query Query
	if err := json.Unmarshal(queryJSONBytes, &query); err != nil {
		log.Printf("Error unmarshalling query JSON for collection '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusBadRequest, "Invalid query JSON format", nil)
		return
	}

	results, err := h.processCollectionQuery(collectionName, query)
	if err != nil {
		log.Printf("Error processing query for collection '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusError, fmt.Sprintf("Failed to execute query: %v", err), nil)
		return
	}

	responseBytes, err := json.Marshal(results)
	if err != nil {
		log.Printf("Error marshalling query results for collection '%s': %v", collectionName, err)
		protocol.WriteResponse(conn, protocol.StatusError, "Failed to marshal query results", nil)
		return
	}

	if err := protocol.WriteResponse(conn, protocol.StatusOk, fmt.Sprintf("OK: Query executed on collection '%s'", collectionName), responseBytes); err != nil {
		log.Printf("Error writing COLLECTION_QUERY response to %s: %v", conn.RemoteAddr(), err)
	}
}

// processCollectionQuery executes a complex query on a collection.
func (h *ConnectionHandler) processCollectionQuery(collectionName string, query Query) (any, error) {
	colStore := h.CollectionManager.GetCollection(collectionName)
	var itemsData map[string][]byte

	// --- CORRECTED: Use the advanced optimizer and set remainingFilter correctly ---
	candidateKeys, usedIndex, remainingFilter := h.findCandidateKeysFromFilter(colStore, query.Filter)

	if usedIndex {
		log.Printf("Query on collection '%s' is using index(es). Candidate keys after intersection: %d", collectionName, len(candidateKeys))
		itemsData = colStore.GetMany(candidateKeys)
	} else {
		log.Printf("Query on collection '%s' is NOT using an index. Falling back to full scan.", collectionName)
		itemsData = colStore.GetAll()
		// CRITICAL FIX: If no index is used, the remaining filter is the original filter.
		remainingFilter = query.Filter
	}
	// --- END CORRECTION ---

	var itemsWithKeys []struct {
		Key string
		Val map[string]any
	}
	for k, vBytes := range itemsData {
		var val map[string]any
		if err := json.Unmarshal(vBytes, &val); err != nil {
			log.Printf("Warning: Failed to unmarshal JSON for key '%s' in collection '%s': %v", k, collectionName, err)
			continue
		}
		itemsWithKeys = append(itemsWithKeys, struct {
			Key string
			Val map[string]any
		}{Key: k, Val: val})
	}

	// 1. Filtering (WHERE clause)
	filteredItems := []struct {
		Key string
		Val map[string]any
	}{}

	// It applies the remaining filter (which contains only the non-indexed conditions, or the full filter if no index was used).
	for _, item := range itemsWithKeys {
		if h.matchFilter(item.Val, remainingFilter) {
			filteredItems = append(filteredItems, item)
		}
	}

	// CRITICAL: The old, duplicated filtering block has been REMOVED from here.

	// Handle DISTINCT early if requested
	if query.Distinct != "" {
		distinctValues := make(map[any]bool)
		var resultList []any
		for _, item := range filteredItems {
			if val, ok := item.Val[query.Distinct]; ok && val != nil {
				if _, seen := distinctValues[val]; !seen {
					distinctValues[val] = true
					resultList = append(resultList, val)
				}
			}
		}
		return resultList, nil
	}

	if query.Count && len(query.Aggregations) == 0 && len(query.GroupBy) == 0 {
		return map[string]int{"count": len(filteredItems)}, nil
	}

	// 2. Aggregations & Group By
	if len(query.Aggregations) > 0 || len(query.GroupBy) > 0 {
		return h.performAggregations(filteredItems, query)
	}

	results := make([]map[string]any, 0, len(filteredItems))
	for _, item := range filteredItems {
		results = append(results, item.Val)
	}

	// 3. Ordering (ORDER BY clause)
	if len(query.OrderBy) > 0 {
		sort.Slice(results, func(i, j int) bool {
			for _, ob := range query.OrderBy {
				valA, okA := results[i][ob.Field]
				valB, okB := results[j][ob.Field]

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
	offset := min(max(query.Offset, 0), len(results))
	results = results[offset:]

	if query.Limit != nil && *query.Limit >= 0 {
		limit := *query.Limit
		if limit == 0 {
			return []map[string]any{}, nil
		}
		if limit > len(results) {
			limit = len(results)
		}
		results = results[:limit]
	}

	return results, nil
}

// findCandidateKeysFromFilter is the new advanced query optimizer.
// It analyzes the filter, uses all possible indexes on AND clauses,
// and returns the candidate keys and any remaining filters that must be applied manually.
func (h *ConnectionHandler) findCandidateKeysFromFilter(colStore store.DataStore, filter map[string]any) (keys []string, usedIndex bool, remainingFilter map[string]any) {
	if len(filter) == 0 {
		return nil, false, filter
	}

	// The main logic is triggered if we find an "and" clause.
	if andConditions, ok := filter["and"].([]any); ok {
		keySets := [][]string{}
		nonIndexedConditions := []any{}

		// 1. Separate indexable conditions from non-indexable ones.
		for _, cond := range andConditions {
			condMap, isMap := cond.(map[string]any)
			if !isMap {
				nonIndexedConditions = append(nonIndexedConditions, cond)
				continue
			}

			field, fieldOk := condMap["field"].(string)
			op, opOk := condMap["op"].(string)
			value := condMap["value"]

			// Is this an equality condition on an indexed field?
			if fieldOk && opOk && op == "=" && colStore.HasIndex(field) {
				// Yes, use the index.
				lookupKeys, _ := colStore.Lookup(field, value)
				keySets = append(keySets, lookupKeys)
				log.Printf("Optimizer: Using index on field '%s'. Found %d candidate keys.", field, len(lookupKeys))
			} else {
				// No, save this condition for the manual filter pass later.
				nonIndexedConditions = append(nonIndexedConditions, condMap)
			}
		}

		// 2. If no indexes were used, return the original filter.
		if len(keySets) == 0 {
			return nil, false, filter
		}

		// 3. Calculate the intersection of all key sets found by the indexes.
		candidateKeys := intersectKeys(keySets)

		// 4. Build the remaining filter.
		// If there are still non-indexed conditions, we'll use them. Otherwise, the filter is empty.
		newFilter := make(map[string]any)
		if len(nonIndexedConditions) > 0 {
			newFilter["and"] = nonIndexedConditions
		}

		return candidateKeys, true, newFilter
	}

	// --- Fallback logic for simple filters (not using AND) ---
	// We keep this to optimize queries that don't use AND.
	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"]
	if fieldOk && opOk && op == "=" && colStore.HasIndex(field) {
		keys, _ := colStore.Lookup(field, value)
		// For a simple filter, there is no remaining filter to apply.
		return keys, true, make(map[string]any)
	}

	// If no index could be used, the full filter is returned.
	return nil, false, filter
}

// matchFilter evaluates an item against a filter condition.
func (h *ConnectionHandler) matchFilter(item map[string]any, filter map[string]any) bool {
	if len(filter) == 0 {
		return true
	}

	// AND condition
	if andConditions, ok := filter["and"].([]any); ok {
		for _, cond := range andConditions {
			if condMap, isMap := cond.(map[string]any); isMap {
				if !h.matchFilter(item, condMap) {
					return false
				}
			} else {
				log.Printf("Warning: Invalid 'and' condition format: %+v", cond)
				return false
			}
		}
		return true
	}

	// OR condition
	if orConditions, ok := filter["or"].([]any); ok {
		for _, cond := range orConditions {
			if condMap, isMap := cond.(map[string]any); isMap {
				if h.matchFilter(item, condMap) {
					return true
				}
			} else {
				log.Printf("Warning: Invalid 'or' condition format: %+v", cond)
				return false
			}
		}
		return false
	}

	// NOT condition
	if notCondition, ok := filter["not"].(map[string]any); ok {
		return !h.matchFilter(item, notCondition)
	}

	// Single field condition
	field, fieldOk := filter["field"].(string)
	op, opOk := filter["op"].(string)
	value := filter["value"]

	if !fieldOk || !opOk {
		log.Printf("Warning: Invalid filter condition (missing field/op): %+v", filter)
		return false
	}

	itemValue, itemValueExists := item[field]

	switch op {
	case "=":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) == 0
	case "!=":
		if !itemValueExists {
			return true
		}
		return compare(itemValue, value) != 0
	case ">":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) > 0
	case ">=":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) >= 0
	case "<":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) < 0
	case "<=":
		if !itemValueExists {
			return false
		}
		return compare(itemValue, value) <= 0
	case "like":
		if !itemValueExists {
			return false
		}
		if sVal, isStr := itemValue.(string); isStr {
			if pattern, isStrPattern := value.(string); isStrPattern {
				pattern = strings.ReplaceAll(regexp.QuoteMeta(pattern), "%", ".*")
				matched, err := regexp.MatchString("(?i)^"+pattern+"$", sVal)
				if err != nil {
					log.Printf("Error in LIKE regex for pattern '%s': %v", pattern, err)
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
		log.Printf("Warning: Unsupported filter operator '%s'", op)
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
