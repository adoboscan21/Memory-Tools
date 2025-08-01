package handler

import (
	"fmt"
)

// ensureIDField unmarshals the value, ensures it's a JSON object (map),
// sets the "_id" field with the provided key, and marshals it back to bytes.
func ensureIDField(value []byte, key string) ([]byte, error) {
	var data map[string]any
	if err := json.Unmarshal(value, &data); err != nil {
		return value, fmt.Errorf("value is not a JSON object, cannot inject _id field: %w", err)
	}

	data["_id"] = key

	updatedValue, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON after injecting _id: %w", err)
	}
	return updatedValue, nil
}
