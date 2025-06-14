package types

type IndexType string

const (
    VALUE_INDEX  IndexType = "VALUE"
    PREFIX_INDEX IndexType = "PREFIX"
    SUFFIX_INDEX IndexType = "SUFFIX"
)

type IndexEntry struct {
    Keys []string `json:"keys"`
}

type ReverseIndex struct {
    Type    IndexType                `json:"type"`
    Entries map[string]*IndexEntry   `json:"entries"`
}
