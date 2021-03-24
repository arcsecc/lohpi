package keyvault

import (
	"fmt"
	"strings"
	"net/http"
	"net/url"
	"net"
	"time"
	"io/ioutil"
	"encoding/json"
)

// Describes configuration to communicate with 
type KeyVaultClientConfig struct {
	ClientID     string `required_true`
	ClientSecret string `required_true`
	TenantID     string `required_true`
	BaseURL      string `required_true`
	SecretName   string `required_true`
}

type TokenResponse struct {
	TokenType   string `json:"token_type"`
	AccessToken string `json:"access_token"`
}

type KeyvaultSecretResponse struct {
	Value string `json:"value"`
	Id    string `json:"id"`
	// attributes
}

type KeyVaultClient struct {
	Token 	string
	config	KeyVaultClientConfig
}

func NewKeyVaultClient(config KeyVaultClientConfig) (*KeyVaultClient, error) {
	const authenticationScope = "https://vault.azure.net/.default"

	var keyVaultransport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 5 * time.Second,
	}

	var keyVaultClient = &http.Client{
		Timeout:   time.Second * 5,
		Transport: keyVaultransport,
	}

	authenticationURL := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", config.TenantID)

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", config.ClientID)
	data.Set("client_secret", config.ClientSecret)
	data.Set("scope", authenticationScope)

	resp, err := keyVaultClient.Post(authenticationURL, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}


	var tr TokenResponse
	errUnMarshal := json.Unmarshal(body, &tr)
	if errUnMarshal != nil {
		return nil, errUnMarshal
	}

	return &KeyVaultClient{
		Token: 	tr.AccessToken,
		config:   config,
	}, nil
}


// GetSecret retrieves a secret from keyvault
func (k *KeyVaultClient) GetSecret(vaultBaseURL, secretName string) (*KeyvaultSecretResponse, error) {
	// Create a Bearer string by appending string access token
	var bearer = "Bearer " + k.Token

	url := fmt.Sprintf("%s/secrets/%s/?api-version=7.0", vaultBaseURL, secretName)

	// Create a new request using http
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// add authorization header to the req
	req.Header.Add("Authorization", bearer)

	// Send req using http Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var sr KeyvaultSecretResponse
	errUnMarshal := json.Unmarshal([]byte(body), &sr)
	if errUnMarshal != nil {
		return nil, errUnMarshal
	}

	return &sr, nil
}