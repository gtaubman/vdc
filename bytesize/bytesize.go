package bytesize

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ByteSize is a uint64 that marshals to/from human-readable strings (e.g. "512M", "5G").
// It also implements flag.Value for CLI use.
type ByteSize uint64

func parse(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, fmt.Errorf("empty value")
	}

	i := strings.IndexFunc(s, func(r rune) bool { return unicode.IsLetter(r) })
	var numStr, suffix string
	if i == -1 {
		numStr = s
	} else {
		numStr, suffix = s[:i], strings.ToUpper(s[i:])
	}

	v, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", numStr)
	}

	var mult float64
	switch suffix {
	case "", "B":
		mult = 1
	case "K", "KB":
		mult = 1 << 10
	case "M", "MB":
		mult = 1 << 20
	case "G", "GB":
		mult = 1 << 30
	case "T", "TB":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("unknown suffix %q (use B, K, M, G, T)", suffix)
	}

	return ByteSize(v * mult), nil
}

func (b ByteSize) String() string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.4gT", float64(b)/float64(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.4gG", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.4gM", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.4gK", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// Set implements flag.Value.
func (b *ByteSize) Set(s string) error {
	v, err := parse(s)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// MarshalJSON emits a human-readable string.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.String())
}

// UnmarshalJSON accepts either a human-readable string ("512M") or a raw number.
func (b *ByteSize) UnmarshalJSON(data []byte) error {
	// Try string first.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		v, err := parse(s)
		if err != nil {
			return err
		}
		*b = v
		return nil
	}

	// Fall back to raw number.
	var n uint64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("bytesize: cannot parse %s", data)
	}
	*b = ByteSize(n)
	return nil
}
