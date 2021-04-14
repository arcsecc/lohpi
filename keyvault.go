package lohpi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Describes configuration to communicate with
type AzureKeyVaultClientConfig struct {
	AzureKeyVaultClientID     string
	AzureKeyVaultClientSecret string
	AzureKeyVaultTenantID     string
}

type tokenResponse struct {
	TokenType   string `json:"token_type"`
	AccessToken string `json:"access_token"`
}

type AzureKeyVaultSecretResponse struct {
	Value string `json:"value"`
	Id    string `json:"id"`
	// attributes
}

type AzureKeyVaultClient struct {
	config *AzureKeyVaultClientConfig
	token  string
}

func NewAzureKeyVaultClient(config *AzureKeyVaultClientConfig) (*AzureKeyVaultClient, error) {
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

	authenticationURL := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", config.AzureKeyVaultTenantID)

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", config.AzureKeyVaultClientID)
	data.Set("client_secret", config.AzureKeyVaultClientSecret)
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

	var tr tokenResponse
	errUnMarshal := json.Unmarshal(body, &tr)
	if errUnMarshal != nil {
		return nil, errUnMarshal
	}

	ss := &AzureKeyVaultClient{
		token:  tr.AccessToken,
		config: config,
	}

	return ss, nil
}

// GetSecret retrieves a secret from keyvault
func (k *AzureKeyVaultClient) GetSecret(vaultBaseURL, secretName string) (*AzureKeyVaultSecretResponse, error) {
	// Create a Bearer string by appending string access token
	var bearer = "Bearer " + k.token

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

	var sr AzureKeyVaultSecretResponse
	errUnMarshal := json.Unmarshal([]byte(body), &sr)
	if errUnMarshal != nil {
		return nil, errUnMarshal
	}

	return &sr, nil
}
