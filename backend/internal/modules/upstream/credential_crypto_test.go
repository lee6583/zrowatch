package upstream

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
)

func testCredentialGCM(t *testing.T) cipher.AEAD {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate test key: %v", err)
	}
	gcm, err := parseCredentialEncryptionKey(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatalf("parse test key: %v", err)
	}
	return gcm
}

func TestCredentialPasswordRoundTripAndWorkspaceAAD(t *testing.T) {
	gcm := testCredentialGCM(t)
	password := "p@ss word/with spaces"
	ciphertext, err := encryptCredentialPassword(gcm, "user-1", "workspace-a", password)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if strings.Contains(ciphertext, password) {
		t.Fatal("ciphertext must not contain plaintext password")
	}

	decrypted, err := decryptCredentialPassword(gcm, "user-1", "workspace-a", ciphertext)
	if err != nil || decrypted != password {
		t.Fatalf("decrypt round-trip failed: value=%q err=%v", decrypted, err)
	}
	if _, err := decryptCredentialPassword(gcm, "user-1", "workspace-b", ciphertext); err != ErrCredentialDecryptFailed {
		t.Fatalf("expected workspace-bound decryption failure, got %v", err)
	}
}

func TestCredentialPayloadKeepsCiphertextButNeverPlaintext(t *testing.T) {
	gcm := testCredentialGCM(t)
	ciphertext, err := encryptCredentialPassword(gcm, "user-1", "workspace-a", "secret-password")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	site := &Site{ID: "site-1", UserID: "user-1", PasswordCiphertext: ciphertext}
	payload := toPayload(site)
	if payload.PasswordCiphertext != ciphertext {
		t.Fatalf("expected ciphertext in internal cache payload")
	}
	roundTrip := fromPayload(payload)
	if roundTrip.PasswordCiphertext != ciphertext {
		t.Fatalf("expected ciphertext to survive cache round-trip")
	}
	if strings.Contains(ciphertext, "secret-password") {
		t.Fatal("cache payload must not contain the plaintext password")
	}
}
