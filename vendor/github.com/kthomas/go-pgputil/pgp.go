package pgputil

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"

	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/armor"
	"golang.org/x/crypto/openpgp/packet"
)

// DecodeArmoredPublicKey decodes an armored PGP public key
func DecodeArmoredPublicKey(armoredKey []byte) (*packet.PublicKey, error) {
	r := bytes.NewReader(armoredKey)
	block, err := armor.Decode(r)
	if err != nil {
		err := fmt.Errorf("Failed to decode armored PGP public key; %s", err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	pktrdr := packet.NewReader(block.Body)
	pkt, err := pktrdr.Next()
	if err != nil {
		err := fmt.Errorf("Failed to decode armored PGP public key; %s", err.Error())
		log.Warning(err.Error())
		return nil, err
	}
	if key, keyOk := pkt.(*packet.PublicKey); keyOk {
		return key, nil
	}

	err = errors.New("Failed to decode armored PGP public key")
	log.Warning(err.Error())
	return nil, err
}

// DecodeArmoredPrivateKey decodes an armored PGP private key
func DecodeArmoredPrivateKey(armoredKey []byte) (*packet.PrivateKey, error) {
	r := bytes.NewReader(armoredKey)
	block, err := armor.Decode(r)
	if err != nil {
		err := fmt.Errorf("Failed to decode armored PGP private key; %s", err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	pktrdr := packet.NewReader(block.Body)
	pkt, err := pktrdr.Next()
	if err != nil {
		err := fmt.Errorf("Failed to decode armored PGP private key; %s", err.Error())
		log.Warning(err.Error())
		return nil, err
	}
	if key, keyOk := pkt.(*packet.PrivateKey); keyOk {
		return key, nil
	}

	err = errors.New("Failed to decode armored PGP private key")
	log.Warning(err.Error())
	return nil, err
}

// PGPPubDecrypt decrypts data
func PGPPubDecrypt(ciphertext []byte) (plaintext []byte, err error) {
	var entityList openpgp.EntityList
	entityList = append(entityList, pgpDefaultEntity)

	buf := bytes.NewBuffer(ciphertext)
	block, err := armor.Decode(buf)
	if err != nil {
		err := fmt.Errorf("Failed to decrypt %d-byte ciphertext; %s", len(ciphertext), err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	i := -1
	md, err := openpgp.ReadMessage(block.Body, entityList, func(keys []openpgp.Key, symmetric bool) ([]byte, error) {
		i++
		if (symmetric && i > 0) || (!symmetric && i > len(keys)-1) {
			return nil, errors.New("Failed in PromptFunction callback provided to openpgp.ReadMessage()")
		}
		if symmetric {
			return pgpPassphrase, nil
		}

		key := keys[i].Entity.PrivateKey
		err := key.Decrypt(pgpPassphrase)
		if err != nil {
			err := fmt.Errorf("Failed to decrypt %d-byte ciphertext; %s", len(ciphertext), err.Error())
			log.Warning(err.Error())
			return nil, err
		}

		return nil, nil
	}, pgpDefaultConfig)

	if err != nil {
		err := fmt.Errorf("Failed to decrypt %d-byte ciphertext; %s", len(ciphertext), err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	plaintext, err = ioutil.ReadAll(md.UnverifiedBody)
	if err != nil {
		err := fmt.Errorf("Failed to decrypt %d-byte ciphertext; %s", len(ciphertext), err.Error())
		log.Warning(err.Error())
		return nil, err
	}
	log.Debugf("Decrypted %d-byte plaintext", len(plaintext))

	return plaintext, nil
}

// PGPPubEncrypt encrypts the given data using the environment-configured public key
func PGPPubEncrypt(plaintext []byte) (ciphertext []byte, err error) {
	if !pgpPublicKey.PubKeyAlgo.CanEncrypt() {
		return nil, errors.New("Public key algorithm does not support encryption")
	}

	buf := bytes.NewBuffer(nil)
	w, err := armor.Encode(buf, "PGP MESSAGE", nil)
	if err != nil {
		return
	}
	defer w.Close()

	pt, err := openpgp.Encrypt(w, []*openpgp.Entity{pgpDefaultEntity}, nil, nil, pgpDefaultConfig)
	if err != nil {
		err := fmt.Errorf("Failed to encrypt %d-byte plaintext; %s", len(plaintext), err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	defer pt.Close()
	_, err = pt.Write(plaintext)
	if err != nil {
		err := fmt.Errorf("Failed to symmetrically encrypt %d-byte plaintext; %s", len(plaintext), err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	pt.Close()
	w.Close()

	ciphertext = buf.Bytes()
	log.Debugf("Encrypted %d-byte ciphertext", len(ciphertext))

	return ciphertext, nil
}

// PGPPubSymmetricallyEncrypt encrypts the given data using the environment-configured public key
func PGPPubSymmetricallyEncrypt(plaintext []byte) (ciphertext []byte, err error) {
	if !pgpPublicKey.PubKeyAlgo.CanEncrypt() {
		return nil, errors.New("Public key algorithm does not support encryption")
	}

	buf := bytes.NewBuffer(nil)
	w, err := armor.Encode(buf, "PGP MESSAGE", nil)
	if err != nil {
		return
	}
	defer w.Close()

	pt, err := openpgp.SymmetricallyEncrypt(w, pgpPassphrase, nil, pgpDefaultConfig)
	if err != nil {
		err := fmt.Errorf("Failed to symmetrically encrypt %d-byte plaintext; %s", len(plaintext), err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	defer pt.Close()
	_, err = pt.Write(plaintext)
	if err != nil {
		err := fmt.Errorf("Failed to symmetrically encrypt %d-byte plaintext; %s", len(plaintext), err.Error())
		log.Warning(err.Error())
		return nil, err
	}

	pt.Close()
	w.Close()

	ciphertext = buf.Bytes()
	log.Debugf("Symmetrically encrypted %d-byte ciphertext", len(ciphertext))

	return ciphertext, nil
}
