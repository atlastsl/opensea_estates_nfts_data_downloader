package helpers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func FetchData(url, apiKey string, respHandler any) error {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Add("accept", "application/json")
	if apiKey != "" {
		if strings.Contains(apiKey, "Bearer") {
			req.Header.Add("Authorization", apiKey)
		} else {
			req.Header.Add("x-api-key", apiKey)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	statusCode := resp.StatusCode
	if statusCode == http.StatusBadRequest {
		return errors.New("error 400 - bas request")
	} else if statusCode == http.StatusInternalServerError || statusCode == http.StatusServiceUnavailable {
		return errors.New("error 500 / 503 - server error")
	} else if statusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("bad response - status code %d", statusCode))
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, respHandler)
	return err
}

func PostData(url, apiKey string, data []byte, respHandler interface{}) error {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		if strings.Contains(apiKey, "Bearer") {
			req.Header.Add("Authorization", apiKey)
		} else {
			req.Header.Add("x-api-key", apiKey)
		}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &respHandler)
	if err != nil {
		return err
	}

	return nil
}
