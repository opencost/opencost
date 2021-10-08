package cloudutil

import (
	"regexp"
	"strings"

	"github.com/kubecost/cost-model/pkg/log"
)

var capitalUnderscore = regexp.MustCompile(`[A-Z]`)
var noSpacePunc = regexp.MustCompile(`[\s]{1,}|[^A-Za-z0-9]`)
var noDupUnderscore = regexp.MustCompile(`_{2,}`)
var noFrontUnderscore = regexp.MustCompile(`(^\_|\_$)`)

// ConvertToGlueColumnFormat takes a string and runs through various regex
// and string replacement statements to convert it to a format compatible
// with AWS Glue and Athena column names.
// Following guidance from AWS provided here ('Column Names' section):
// https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/run-athena-sql.html
// It returns a string containing the column name in proper column name format and length.
func ConvertToGlueColumnFormat(columnName string) string {
	log.Debugf("Converting string \"%s\" to proper AWS Glue column name.", columnName)

	// An underscore is added in front of uppercase letters
	final := capitalUnderscore.ReplaceAllString(columnName, `_$0`)

	// Any non-alphanumeric characters are replaced with an underscore
	final = noSpacePunc.ReplaceAllString(final, "_")

	// Duplicate underscores are removed
	final = noDupUnderscore.ReplaceAllString(final, "_")

	// Any leading and trailing underscores are removed
	final = noFrontUnderscore.ReplaceAllString(final, "")

	// Uppercase to lowercase
	final = strings.ToLower(final)

	// Longer column name than expected - remove _ left to right
	allowedColLen := 128
	underscoreToRemove := len(final) - allowedColLen
	if underscoreToRemove > 0 {
		final = strings.Replace(final, "_", "", underscoreToRemove)
	}

	// If removing all of the underscores still didn't
	// make the column name < 128 characters, trim it!
	if len(final) > allowedColLen {
		final = final[:allowedColLen]
	}

	log.Debugf("Column name being returned: \"%s\". Length: \"%d\".", final, len(final))

	return final
}
