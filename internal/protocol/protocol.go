package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// CommandType defines the type of operation requested by the client.
type CommandType byte

const (
	// Main Store Commands
	CmdSet CommandType = iota + 1 // SET key, value, ttl
	CmdGet                        // GET key

	// Collection Management Commands
	CmdCollectionCreate // CREATE_COLLECTION collectionName
	CmdCollectionDelete // DELETE_COLLECTION collectionName
	CmdCollectionList   // LIST_COLLECTIONS

	// Collection Item Commands
	CmdCollectionItemSet    // SET_COLLECTION_ITEM collectionName, key, value, ttl
	CmdCollectionItemGet    // GET_COLLECTION_ITEM collectionName, key
	CmdCollectionItemDelete // DELETE_COLLECTION_ITEM collectionName, key
	CmdCollectionItemList   // LIST_COLLECTION_ITEMS collectionName
	CmdCollectionQuery      // NEW: QUERY_COLLECTION collectionName, query_json (for SQL-like operations)

	// Authentication Commands
	CmdAuthenticate       // AUTH username, password
	CmdChangeUserPassword // CHANGE_USER_PASSWORD target_username, new_password (formerly CmdUpdatePassword)
)

// ResponseStatus defines the status of a server response.
type ResponseStatus byte

const (
	StatusOk           ResponseStatus = iota + 1
	StatusNotFound                    // Not found status.
	StatusError                       // Generic error status.
	StatusBadCommand                  // Bad command format.
	StatusUnauthorized                // Unauthorized access.
	StatusBadRequest                  // Bad request (e.g., empty key/name).
)

var ByteOrder = binary.LittleEndian

// WriteResponse sends a structured binary response over the connection.
func WriteResponse(w io.Writer, status ResponseStatus, msg string, data []byte) error {
	// Write status (1 byte).
	if _, err := w.Write([]byte{byte(status)}); err != nil {
		return fmt.Errorf("failed to write status: %w", err)
	}

	// Write message length (4 bytes) and message.
	if err := binary.Write(w, ByteOrder, uint32(len(msg))); err != nil {
		return fmt.Errorf("failed to write message length: %w", err)
	}
	if _, err := w.Write([]byte(msg)); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	// Write data length (4 bytes) and data.
	if err := binary.Write(w, ByteOrder, uint32(len(data))); err != nil {
		return fmt.Errorf("failed to write data length: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	return nil
}

// ReadCommandType reads the command type from the connection.
func ReadCommandType(r io.Reader) (CommandType, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, fmt.Errorf("failed to read command type: %w", err)
	}
	return CommandType(buf[0]), nil
}

// ReadString reads a length-prefixed string from the connection.
func ReadString(r io.Reader) (string, error) {
	var strLen uint32
	if err := binary.Read(r, ByteOrder, &strLen); err != nil {
		return "", fmt.Errorf("failed to read string length: %w", err)
	}
	strBytes := make([]byte, strLen)
	if _, err := io.ReadFull(r, strBytes); err != nil {
		return "", fmt.Errorf("failed to read string bytes: %w", err)
	}
	return string(strBytes), nil
}

// WriteString writes a length-prefixed string to the connection.
func WriteString(w io.Writer, s string) error {
	if err := binary.Write(w, ByteOrder, uint32(len(s))); err != nil {
		return fmt.Errorf("failed to write string length: %w", err)
	}
	if _, err := w.Write([]byte(s)); err != nil {
		return fmt.Errorf("failed to write string: %w", err)
	}
	return nil
}

// ReadBytes reads length-prefixed bytes from the connection.
func ReadBytes(r io.Reader) ([]byte, error) {
	var byteLen uint32
	if err := binary.Read(r, ByteOrder, &byteLen); err != nil {
		return nil, fmt.Errorf("failed to read bytes length: %w", err)
	}
	byteData := make([]byte, byteLen)
	if _, err := io.ReadFull(r, byteData); err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}
	return byteData, nil
}

// WriteBytes writes length-prefixed bytes to the connection.
func WriteBytes(w io.Writer, b []byte) error {
	if err := binary.Write(w, ByteOrder, uint32(len(b))); err != nil {
		return fmt.Errorf("failed to write bytes length: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("failed to write bytes: %w", err)
	}
	return nil
}

// WriteSetCommand writes a SET command to the connection.
// Format: [CmdSet (1 byte)] [KeyLength (4 bytes)] [Key] [ValueLength (4 bytes)] [Value] [TTLSeconds (8 bytes)]
func WriteSetCommand(w io.Writer, key string, value []byte, ttl time.Duration) error {
	if _, err := w.Write([]byte{byte(CmdSet)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	if err := WriteBytes(w, value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}
	if err := binary.Write(w, ByteOrder, int64(ttl.Seconds())); err != nil {
		return fmt.Errorf("failed to write TTL seconds: %w", err)
	}
	return nil
}

// ReadSetCommand reads a SET command from the connection.
func ReadSetCommand(r io.Reader) (key string, value []byte, ttl time.Duration, err error) {
	key, err = ReadString(r)
	if err != nil {
		return "", nil, 0, fmt.Errorf("failed to read key: %w", err)
	}
	value, err = ReadBytes(r)
	if err != nil {
		return "", nil, 0, fmt.Errorf("failed to read value: %w", err)
	}
	var ttlSeconds int64
	if err := binary.Read(r, ByteOrder, &ttlSeconds); err != nil {
		return "", nil, 0, fmt.Errorf("failed to read TTL seconds: %w", err)
	}
	ttl = time.Duration(ttlSeconds) * time.Second
	return key, value, ttl, nil
}

// WriteGetCommand writes a GET command to the connection.
// Format: [CmdGet (1 byte)] [KeyLength (4 bytes)] [Key]
func WriteGetCommand(w io.Writer, key string) error {
	if _, err := w.Write([]byte{byte(CmdGet)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	return nil
}

// ReadGetCommand reads a GET command from the connection.
func ReadGetCommand(r io.Reader) (key string, err error) {
	key, err = ReadString(r)
	if err != nil {
		return "", fmt.Errorf("failed to read key: %w", err)
	}
	return key, nil
}

// WriteCollectionCreateCommand writes a CREATE_COLLECTION command to the connection.
// Format: [CmdCollectionCreate (1 byte)] [CollectionNameLength (4 bytes)] [CollectionName]
func WriteCollectionCreateCommand(w io.Writer, collectionName string) error {
	if _, err := w.Write([]byte{byte(CmdCollectionCreate)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, collectionName); err != nil {
		return fmt.Errorf("failed to write collection name: %w", err)
	}
	return nil
}

// ReadCollectionCreateCommand reads a CREATE_COLLECTION command from the connection.
func ReadCollectionCreateCommand(r io.Reader) (collectionName string, err error) {
	collectionName, err = ReadString(r)
	if err != nil {
		return "", fmt.Errorf("failed to read collection name: %w", err)
	}
	return collectionName, nil
}

// WriteCollectionDeleteCommand writes a DELETE_COLLECTION command to the connection.
// Format: [CmdCollectionDelete (1 byte)] [CollectionNameLength (4 bytes)] [CollectionName]
func WriteCollectionDeleteCommand(w io.Writer, collectionName string) error {
	if _, err := w.Write([]byte{byte(CmdCollectionDelete)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, collectionName); err != nil {
		return fmt.Errorf("failed to write collection name: %w", err)
	}
	return nil
}

// ReadCollectionDeleteCommand reads a DELETE_COLLECTION command from the connection.
func ReadCollectionDeleteCommand(r io.Reader) (collectionName string, err error) {
	collectionName, err = ReadString(r)
	if err != nil {
		return "", fmt.Errorf("failed to read collection name: %w", err)
	}
	return collectionName, nil
}

// WriteCollectionListCommand writes a LIST_COLLECTIONS command to the connection.
// Format: [CmdCollectionList (1 byte)]
func WriteCollectionListCommand(w io.Writer) error {
	if _, err := w.Write([]byte{byte(CmdCollectionList)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	return nil
}

// WriteCollectionItemSetCommand writes a SET_COLLECTION_ITEM command to the connection.
// Format: [CmdCollectionItemSet (1 byte)] [ColNameLength] [ColName] [KeyLength] [Key] [ValueLength] [Value] [TTLSeconds]
func WriteCollectionItemSetCommand(w io.Writer, collectionName, key string, value []byte, ttl time.Duration) error {
	if _, err := w.Write([]byte{byte(CmdCollectionItemSet)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, collectionName); err != nil {
		return fmt.Errorf("failed to write collection name: %w", err)
	}
	if err := WriteString(w, key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	if err := WriteBytes(w, value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}
	if err := binary.Write(w, ByteOrder, int64(ttl.Seconds())); err != nil {
		return fmt.Errorf("failed to write TTL seconds: %w", err)
	}
	return nil
}

// ReadCollectionItemSetCommand reads a SET_COLLECTION_ITEM command from the connection.
func ReadCollectionItemSetCommand(r io.Reader) (collectionName, key string, value []byte, ttl time.Duration, err error) {
	collectionName, err = ReadString(r)
	if err != nil {
		return "", "", nil, 0, fmt.Errorf("failed to read collection name: %w", err)
	}
	key, err = ReadString(r)
	if err != nil {
		return "", "", nil, 0, fmt.Errorf("failed to read key: %w", err)
	}
	value, err = ReadBytes(r)
	if err != nil {
		return "", "", nil, 0, fmt.Errorf("failed to read value: %w", err)
	}
	var ttlSeconds int64
	if err := binary.Read(r, ByteOrder, &ttlSeconds); err != nil {
		return "", "", nil, 0, fmt.Errorf("failed to read TTL seconds: %w", err)
	}
	ttl = time.Duration(ttlSeconds) * time.Second
	return collectionName, key, value, ttl, nil
}

// WriteCollectionItemGetCommand writes a GET_COLLECTION_ITEM command to the connection.
// Format: [CmdCollectionItemGet (1 byte)] [ColNameLength] [ColName] [KeyLength] [Key]
func WriteCollectionItemGetCommand(w io.Writer, collectionName, key string) error {
	if _, err := w.Write([]byte{byte(CmdCollectionItemGet)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, collectionName); err != nil {
		return fmt.Errorf("failed to write collection name: %w", err)
	}
	if err := WriteString(w, key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	return nil
}

// ReadCollectionItemGetCommand reads a GET_COLLECTION_ITEM command from the connection.
func ReadCollectionItemGetCommand(r io.Reader) (collectionName, key string, err error) {
	collectionName, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read collection name: %w", err)
	}
	key, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read key: %w", err)
	}
	return collectionName, key, nil
}

// WriteCollectionItemDeleteCommand writes a DELETE_COLLECTION_ITEM command to the connection.
// Format: [CmdCollectionItemDelete (1 byte)] [ColNameLength] [ColName] [KeyLength] [Key]
func WriteCollectionItemDeleteCommand(w io.Writer, collectionName, key string) error {
	if _, err := w.Write([]byte{byte(CmdCollectionItemDelete)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, collectionName); err != nil {
		return fmt.Errorf("failed to write collection name: %w", err)
	}
	if err := WriteString(w, key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	return nil
}

// ReadCollectionItemDeleteCommand reads a DELETE_COLLECTION_ITEM command from the connection.
func ReadCollectionItemDeleteCommand(r io.Reader) (collectionName, key string, err error) {
	collectionName, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read collection name: %w", err)
	}
	key, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read key: %w", err)
	}
	return collectionName, key, nil
}

// WriteCollectionItemListCommand writes a LIST_COLLECTION_ITEMS command to the connection.
// Format: [CmdCollectionItemList (1 byte)] [ColNameLength] [ColName]
func WriteCollectionItemListCommand(w io.Writer, collectionName string) error {
	if _, err := w.Write([]byte{byte(CmdCollectionItemList)}); err != nil {
		return fmt.Errorf("failed to write command type: %w", err)
	}
	if err := WriteString(w, collectionName); err != nil {
		return fmt.Errorf("failed to write collection name: %w", err)
	}
	return nil
}

// ReadCollectionItemListCommand reads a LIST_COLLECTION_ITEMS command from the connection.
func ReadCollectionItemListCommand(r io.Reader) (collectionName string, err error) {
	collectionName, err = ReadString(r)
	if err != nil {
		return "", fmt.Errorf("failed to read collection name: %w", err)
	}
	return collectionName, nil
}

// WriteAuthenticateCommand writes an AUTH command to the connection.
// Format: [CmdAuthenticate (1 byte)] [UsernameLength (4 bytes)] [Username] [PasswordLength (4 bytes)] [Password]
func WriteAuthenticateCommand(w io.Writer, username, password string) error {
	if _, err := w.Write([]byte{byte(CmdAuthenticate)}); err != nil {
		return fmt.Errorf("failed to write command type (authenticate): %w", err)
	}
	if err := WriteString(w, username); err != nil {
		return fmt.Errorf("failed to write username (authenticate): %w", err)
	}
	if err := WriteString(w, password); err != nil {
		return fmt.Errorf("failed to write password (authenticate): %w", err)
	}
	return nil
}

// ReadAuthenticateCommand reads an AUTH command from the connection.
func ReadAuthenticateCommand(r io.Reader) (username, password string, err error) {
	username, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read username (authenticate): %w", err)
	}
	password, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read password (authenticate): %w", err)
	}
	return username, password, nil
}

// WriteChangeUserPasswordCommand writes a CHANGE_USER_PASSWORD command to the connection.
// Format: [CmdChangeUserPassword (1 byte)] [TargetUsernameLength (4 bytes)] [TargetUsername] [NewPasswordLength (4 bytes)] [NewPassword]
func WriteChangeUserPasswordCommand(w io.Writer, targetUsername, newPassword string) error {
	if _, err := w.Write([]byte{byte(CmdChangeUserPassword)}); err != nil {
		return fmt.Errorf("failed to write command type (change user password): %w", err)
	}
	if err := WriteString(w, targetUsername); err != nil {
		return fmt.Errorf("failed to write target username (change user password): %w", err)
	}
	if err := WriteString(w, newPassword); err != nil {
		return fmt.Errorf("failed to write new password (change user password): %w", err)
	}
	return nil
}

// ReadChangeUserPasswordCommand reads a CHANGE_USER_PASSWORD command from the connection.
func ReadChangeUserPasswordCommand(r io.Reader) (targetUsername, newPassword string, err error) {
	targetUsername, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read target username (change user password): %w", err)
	}
	newPassword, err = ReadString(r)
	if err != nil {
		return "", "", fmt.Errorf("failed to read new password (change user password): %w", err)
	}
	return targetUsername, newPassword, nil
}

// WriteCollectionQueryCommand writes a QUERY_COLLECTION command to the connection.
// Format: [CmdCollectionQuery (1 byte)] [CollectionNameLength (4 bytes)] [CollectionName] [QueryJSONLength (4 bytes)] [QueryJSON]
func WriteCollectionQueryCommand(w io.Writer, collectionName string, queryJSON []byte) error {
	if _, err := w.Write([]byte{byte(CmdCollectionQuery)}); err != nil {
		return fmt.Errorf("failed to write command type (collection query): %w", err)
	}
	if err := WriteString(w, collectionName); err != nil {
		return fmt.Errorf("failed to write collection name (collection query): %w", err)
	}
	if err := WriteBytes(w, queryJSON); err != nil {
		return fmt.Errorf("failed to write query JSON (collection query): %w", err)
	}
	return nil
}

// ReadCollectionQueryCommand reads a QUERY_COLLECTION command from the connection.
func ReadCollectionQueryCommand(r io.Reader) (collectionName string, queryJSON []byte, err error) {
	collectionName, err = ReadString(r)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read collection name (collection query): %w", err)
	}
	queryJSON, err = ReadBytes(r)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read query JSON (collection query): %w", err)
	}
	return collectionName, queryJSON, nil
}
