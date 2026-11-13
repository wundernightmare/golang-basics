package secret_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/tracehubmmp/golang-basics/libs/secret"
)

func Example() {
	type Config struct {
		Host     string        `json:"host"`
		Password secret.String `json:"password"`
	}

	var cfg Config

	_ = json.Unmarshal([]byte(`{"host":"db","password":"hunter2"}`), &cfg)

	// Printing, logging and marshalling the config are all safe.
	fmt.Printf("%+v\n", cfg)

	out, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(out))

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}

			return a
		},
	}))
	log.Info("boot", "password", cfg.Password)

	// The value is one explicit call away, at the point of use.
	fmt.Println(cfg.Password.Expose())

	// Output:
	// {Host:db Password:[redacted:f52fbd32]}
	// {"host":"db","password":"[redacted:f52fbd32]"}
	// level=INFO msg=boot password=[redacted:f52fbd32]
	// hunter2
}
