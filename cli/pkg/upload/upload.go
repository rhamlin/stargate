package upload

import (
	"errors"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/PuerkitoBio/purell"
)

// Upload posts the contents of a file to an url
func Upload(path string, url string) error {
	url, err := purell.NormalizeURLString(url, purell.FlagsUnsafeGreedy)
	if err != nil {
		return err
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	resp, err := http.Post(url, "application/hocon", file)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		message, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New("Error:\n" + string(message))
	}

	return nil
}
