package upstream

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const upstreamCredentialCiphertextPrefix = "v1:"

var (
	ErrCredentialEncryptionUnavailable = errors.New("admin.upstream.errors.credentialEncryptionUnavailable")
	ErrCredentialDecryptFailed         = errors.New("admin.upstream.errors.credentialDecryptFailed")
)

func parseCredentialEncryptionKey(raw string) (cipher.AEAD, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return nil, errors.New("invalid upstream credential encryption key: not valid base64")
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("invalid upstream credential encryption key: expected 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("invalid upstream credential encryption key")
	}
	return cipher.NewGCM(block)
}

func credentialAAD(userID, adminAccountID string) []byte {
	return []byte(strings.TrimSpace(userID) + "\x00" + strings.TrimSpace(adminAccountID))
}

func encryptCredentialPassword(gcm cipher.AEAD, userID, adminAccountID, password string) (string, error) {
	if gcm == nil {
		return "", ErrCredentialEncryptionUnavailable
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(password), credentialAAD(userID, adminAccountID))
	return upstreamCredentialCiphertextPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

func decryptCredentialPassword(gcm cipher.AEAD, userID, adminAccountID, stored string) (string, error) {
	if gcm == nil {
		return "", ErrCredentialEncryptionUnavailable
	}
	if !strings.HasPrefix(stored, upstreamCredentialCiphertextPrefix) {
		return "", ErrCredentialDecryptFailed
	}
	sealed, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, upstreamCredentialCiphertextPrefix))
	if err != nil || len(sealed) < gcm.NonceSize() {
		return "", ErrCredentialDecryptFailed
	}
	nonce, ciphertext := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	password, err := gcm.Open(nil, nonce, ciphertext, credentialAAD(userID, adminAccountID))
	if err != nil {
		return "", ErrCredentialDecryptFailed
	}
	return string(password), nil
}
