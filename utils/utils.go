package utils

import (
	"regexp"
	"strconv"
)

func GetRegisterId(abstractionId string) string {
	// group the inside of the brackets
	re := regexp.MustCompile(`\[(.*)\]`)

	tokens := re.FindStringSubmatch(abstractionId)

	// return the first matched group
	return tokens[1]
}

func Int32ToString(i int32) string {
	return strconv.Itoa(int(i))
}
