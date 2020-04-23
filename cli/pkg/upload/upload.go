package upload

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/PuerkitoBio/purell"
)

func isSuccess(statusCode int) bool {
	return statusCode == 200
}

func isFailure(statusCode int) bool {
	return statusCode != 200
}

type response struct {
	Code    string
	Message string
	Path    string
	Status  string
	Error   []string
}

// Upload posts the contents of a file to an url
func Upload(path string, url string) (string, error) {
	url, err := purell.NormalizeURLString(url, purell.FlagsUnsafeGreedy)
	if err != nil {
		return "", err
	}

	file, err := os.Open(path)
	defer file.Close()

	resp, err := http.Post(url, "text/plain", file)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	var responseBody response
	json.Unmarshal(body, &responseBody)

	if len(responseBody.Message) > 0 {
		return "", errors.New(responseBody.Message)
	}

	return responseBody.Path, err
}
