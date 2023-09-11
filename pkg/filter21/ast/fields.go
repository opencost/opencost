package ast

// FieldType is an enumeration of specific types relevant to lexing and
// parsing a filter.
type FieldType int

const (
	FieldTypeDefault FieldType = iota
	FieldTypeSlice
	FieldTypeMap
	FieldTypeAlias
)

// Field is a Lexer input which acts as a mapping of identifiers used to lex/parse filters.
type Field struct {
	// Name contains the name of the specific field as it appears in language.
	Name string

	fieldType FieldType
}

// Field equivalence is determined by name and type.
func (f *Field) Equal(other *Field) bool {
	if f == nil || other == nil {
		return false
	}

	return f.Name == other.Name && f.fieldType == other.fieldType
}

// IsSlice returns true if the field is a slice. This instructs the lexer that the field
// should allow contains operations.
func (f *Field) IsSlice() bool {
	return f.fieldType == FieldTypeSlice
}

// IsMap returns true if the field is a map. This instructs the lexer that the field should
// allow keyed-access operations.
func (f *Field) IsMap() bool {
	return f.fieldType == FieldTypeMap
}

// IsAlias returns true if the field is an alias type. This instructs the lexer that the field
// is an alias for custom logical resolution by an external compiler.
func (f *Field) IsAlias() bool {
	return f.fieldType == FieldTypeAlias
}

// NewField creates a default string field using the provided name.
func NewField[T ~string](name T) *Field {
	return &Field{
		Name:      string(name),
		fieldType: FieldTypeDefault,
	}
}

// NewSliceField creates a slice field using the provided name.
func NewSliceField[T ~string](name T) *Field {
	return &Field{
		Name:      string(name),
		fieldType: FieldTypeSlice,
	}
}

// NewMapField creates a new map field using the provided name.
func NewMapField[T ~string](name T) *Field {
	return &Field{
		Name:      string(name),
		fieldType: FieldTypeMap,
	}
}

// NewAliasField creates a new alias field using the provided name.
func NewAliasField[T ~string](name T) *Field {
	return &Field{
		Name:      string(name),
		fieldType: FieldTypeAlias,
	}
}
