package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	redis "github.com/go-redis/redis/v8"
)

func main() {
	/* Variables */
	var (
		ctx            context.Context = context.Background()
		err            error
		fileName       string = "migration.dat"
		srcServerAddr  string = "localhost:6379"
		destServerAddr string = "localhost:6380"

		cursorIdx    uint64 = 0
		matchPattern string = ""
		scanCount    int64  = 1
	)

	defer func() {
		if err != nil {
			log.Panicf("[ERROR] %v \n", err)
			log.Panicf("Last cursor index: %v \n", cursorIdx)
			ioutil.WriteFile(
				fileName,
				[]byte(
					fmt.Sprintf("%v\n%v",
						cursorIdx,
						fmt.Sprintf("[ERROR] %v", err),
					),
				), 0644,
			)
		} else {
			log.Printf(
				"Redis %s has successfully migrated to Redis %s. \n",
				srcServerAddr, destServerAddr,
			)
		}
	}()

	/* Initialize Redis Server */
	fileContent, _ := ioutil.ReadFile(fileName)
	cursorIdx, _ = strconv.ParseUint(strings.Split(string(fileContent), "\n")[0], 10, 64)

	srcRdb := redis.NewClient(&redis.Options{
		Addr: srcServerAddr,
	})
	err = srcRdb.Ping(ctx).Err()
	if err != nil {
		return
	}

	destRdb := redis.NewClient(&redis.Options{
		Addr: destServerAddr,
	})
	err = destRdb.Ping(ctx).Err()
	if err != nil {
		return
	}

	/* Migration Script */
	for {
		var keys []string
		srcScanRes := srcRdb.Scan(ctx, cursorIdx, matchPattern, scanCount)
		keys, cursorIdx, err = srcScanRes.Result()
		if err != nil {
			return
		}

		for _, key := range keys {
			var itemType string
			srcTypeRes := srcRdb.Type(ctx, key)
			itemType, err = srcTypeRes.Result()
			if err != nil {
				return
			}
			destRdb.Del(ctx, key)

			if itemType == "set" {
				var members []string
				srcSMembersRes := srcRdb.SMembers(ctx, key)
				members, err = srcSMembersRes.Result()
				if err != nil {
					return
				}

				destSAddRes := destRdb.SAdd(ctx, key, members)
				_, err = destSAddRes.Result()
				if err != nil {
					return
				}

				log.Printf("SMEMBERS - Key %s migrated.\n", key)
			}
		}

		if cursorIdx == 0 {
			break
		}
	}

	return
}
