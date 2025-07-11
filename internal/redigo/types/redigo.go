package types

type InitializationStep struct {
	Name     string
	Function func() error
}

type IndexOperation struct {
	Name    string
	Index   *ReverseIndex
	GetKeys func(string) []string
}