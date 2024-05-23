package utils

import (
	"amcds/pb"
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

type ProcessMap map[string]*pb.ProcessId

func GetProcessKey(p *pb.ProcessId) string {
	return p.Owner + Int32ToString(p.Index)
}

func GetMaxRank(processes ProcessMap) *pb.ProcessId {
	var maxRank *pb.ProcessId

	for _, v := range processes {
		if maxRank == nil || v.Rank > maxRank.Rank {
			maxRank = v
			continue
		}
	}

	return maxRank
}

func GetMaxRankSlice(processes []*pb.ProcessId) *pb.ProcessId {
	var maxRank *pb.ProcessId

	for _, v := range processes {
		if maxRank == nil || v.Rank > maxRank.Rank {
			maxRank = v
			continue
		}
	}

	return maxRank
}
