package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/chenhg5/collection"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var i = flag.Bool("i", false, "output csv file of issues statistics")
var t = flag.String("t", "", "access token to get raw data")
var dbHost = flag.String("host", "", "database host")
var dbPort = flag.Int64("port", 0, "database port")
var dbUser = flag.String("user", "", "database user")
var dbPassword = flag.String("pass", "", "database password")
var dbName = flag.String("database", "", "database name")

func JsonToMap(str string) map[string]interface{} {
	var tempMap map[string]interface{}
	err := json.Unmarshal([]byte(str), &tempMap)
	if err != nil {
		panic(err)
	}
	return tempMap
}

func JsonToSlice(str string) []map[string]interface{} {
	var tempSlice []map[string]interface{}
	err := json.Unmarshal([]byte(str), &tempSlice)
	if err != nil {
		panic(err)
	}
	return tempSlice
}

func CountCreatedDays(createdAt string) int64 {
	createdStr := strings.Replace(createdAt[:len(createdAt)-6], "T", " ", -1)
	createdTime, _ := time.Parse("2006-01-02 15:04:05", createdStr)
	duration := time.Now().Unix() - createdTime.Unix()
	createdDays := duration / 86400
	return createdDays
}

func GetSigsMapping() (map[string][]string, map[string]string) {
	fmt.Println("Starting to get mapping between sigs and repos.")
	url := "https://gitee.com/api/v5/repos/openeuler/community/git/trees/master?access_token={access_token}&recursive=1"
	url = strings.Replace(url, "{access_token}", fmt.Sprintf("%v", os.Getenv("AccessToken")), -1)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
		return nil, nil
	}
	body, _ := ioutil.ReadAll(resp.Body)
	err = resp.Body.Close()
	if err != nil {
		return nil, nil
	}
	treeMap := JsonToMap(string(body))
	sigs := map[string][]string{}
	repos := map[string]string{}
	for _, value := range treeMap["tree"].([]interface{}) {
		path := value.(map[string]interface{})["path"]
		pathSlices := strings.Split(path.(string), "/")
		if len(pathSlices) == 5 && strings.HasPrefix(path.(string), "sig") &&
			strings.HasSuffix(path.(string), ".yaml") {
			sigName := pathSlices[1]
			repoName := pathSlices[2] + "/" + pathSlices[4][:len(pathSlices[4])-5]
			repos[repoName] = sigName
			_, ok := sigs[sigName]
			if !ok {
				sigs[sigName] = []string{repoName}
			} else {
				sigs[sigName] = append(sigs[sigName], repoName)
			}
		}
	}
	fmt.Println("Success to get mapping between sigs and repos.")
	return sigs, repos
}

func GetSigByRepo(repos map[string]string, repo string) string {
	sig, ok := repos[repo]
	if !ok {
		return ""
	}
	return sig
}

func UpdatePullsStatus(status string, insertStr string) string {
	if status == "待合入" {
		status = insertStr
	} else {
		status += "、" + insertStr
	}
	return status
}

func GetPullStatus(tags []string, status string) string {
	if collection.Collect(tags).Contains("openeuler-cla/yes") == false {
		status = UpdatePullsStatus(status, "CLA认证失败")
	}
	if collection.Collect(tags).Contains("ci_failed") == true {
		status = UpdatePullsStatus(status, "门禁检查失败")
	}
	if collection.Collect(tags).Contains("kind/wait_for_update") == true {
		status = UpdatePullsStatus(status, "等待更新")
	}
	return status
}

func GenerateReportCsv(issues [][]string, fileName string) {
	timeStr := time.Now().Format("20060102150405")
	filePath := fileName + "-" + timeStr + ".csv"
	nfs, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("文件打开失败", err)
	}
	defer func(fs *os.File) {
		err := fs.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(nfs)
	_, _ = nfs.Seek(0, io.SeekEnd)
	w := csv.NewWriter(nfs)
	w.Comma = ','
	w.UseCRLF = true
	err = w.WriteAll(issues)
	if err != nil {
		return
	}
}

func UpdateEnterpriseIssues(token string, db *sql.DB) {
	_, repos := GetSigsMapping()
	var issuesAll [][]string
	csvHeader := []string{
		"SIG", "REPO", "LINK", "TYPE", "STATUS", "DAYS", "LABELS", "AUTHOR", "TITLE",
	}
	issuesAll = append(issuesAll, csvHeader)
	page := 1
	for {
		url := "https://gitee.com/api/v5/enterprises/open_euler/issues?state=open&sort=created&direction=asc" +
			"&page={page}&per_page=100&access_token={access_token}"
		url = strings.Replace(url, "{page}", fmt.Sprintf("%v", page), -1)
		url = strings.Replace(url, "{access_token}", fmt.Sprintf("%v", token), -1)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println(err)
			return
		}
		body, _ := ioutil.ReadAll(resp.Body)
		err = resp.Body.Close()
		if err != nil {
			return
		}
		if len(string(body)) == 2 {
			break
		}
		issuesSlice := JsonToSlice(string(body))
		for _, issue := range issuesSlice {
			var issueSingle []string
			repository := issue["repository"]
			if repository == nil {
				continue
			}
			htmlUrl := issue["html_url"]
			org := strings.Split(htmlUrl.(string), "/")[3]
			if org != "src-openeuler" && org != "openeuler" {
				continue
			}
			title := issue["title"]
			repo := issue["repository"].(map[string]interface{})["full_name"]
			author := issue["user"].(map[string]interface{})["login"]
			number := issue["number"]
			issueType := issue["issue_type"]
			detailState := issue["issue_state_detail"].(map[string]interface{})["title"]
			createdAt := issue["created_at"]
			sig := GetSigByRepo(repos, repo.(string))
			labels := issue["labels"]
			tags := make([]string, 0)
			if labels != nil {
				for _, label := range labels.([]interface{}) {
					tags = append(tags, label.(map[string]interface{})["name"].(string))
				}
			}
			issueExists := SearchIssue(db, htmlUrl.(string))
			if issueExists == true {
				fmt.Println("Issue已存在，更新记录：", htmlUrl)
				updateSql := fmt.Sprintf("update issue set sig='%s', issueType='%s', issueState='%s' where link='%s'", sig, issueType, detailState, htmlUrl)
				result, err := db.Exec(updateSql)
				if err != nil {
					fmt.Println("Update failed:", err, ", sql:", updateSql)
				}
				id, err := result.RowsAffected()
				if err != nil {
					fmt.Println("RowsAffected failed:", err)
				}
				fmt.Println("Update successfully:", id)
			} else {
				fmt.Println("Issue不存在，插入记录：", htmlUrl)
				r, err := db.Exec("insert into issue(sig, repo, link, number, issueType, issueState, author, createdAt)values(?, ?, ?, ?, ?, ?, ?, ?)", sig, repo, htmlUrl, number, issueType, detailState, author, createdAt)
				if err != nil {
					fmt.Println("exec failed, ", err)
					return
				}
				id, err := r.LastInsertId()
				if err != nil {
					fmt.Println("exec failed, ", err)
					return
				}
				fmt.Println("insert successfully:", id)
			}
			issueSingle = append(issueSingle, sig)
			issueSingle = append(issueSingle, repo.(string))
			issueSingle = append(issueSingle, htmlUrl.(string))
			issueSingle = append(issueSingle, issueType.(string))
			issueSingle = append(issueSingle, detailState.(string))
			createdDays := CountCreatedDays(createdAt.(string))
			issueSingle = append(issueSingle, strconv.FormatInt(createdDays, 10))
			tagsStr, _ := json.Marshal(tags)
			issueSingle = append(issueSingle, string(tagsStr))
			issueSingle = append(issueSingle, author.(string))
			issueSingle = append(issueSingle, title.(string))
			issuesAll = append(issuesAll, issueSingle)
		}
		page += 1
	}
	GenerateReportCsv(issuesAll, "issues")
}

func UpdateEnterprisePulls(token string, db *sql.DB) {
	_, repos := GetSigsMapping()
	var pullsAll [][]string
	csvHeader := []string{
		"SIG", "LINK", "DAYS", "STATUS", "AUTHOR",
	}
	pullsAll = append(pullsAll, csvHeader)
	page := 1
	for {
		url := "https://gitee.com/api/v5/enterprise/open_euler/pull_requests?state=open&sort=created&direction=desc" +
			"&page={page}&per_page=100&access_token={access_token}"
		url = strings.Replace(url, "{page}", fmt.Sprintf("%v", page), -1)
		url = strings.Replace(url, "{access_token}", fmt.Sprintf("%v", token), -1)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println(err)
			return
		}
		body, _ := ioutil.ReadAll(resp.Body)
		err = resp.Body.Close()
		if err != nil {
			return
		}
		if len(string(body)) == 2 {
			break
		}
		pullsSlice := JsonToSlice(string(body))
		for _, pull := range pullsSlice {
			var pullSingle []string
			var status = "待合入"
			htmlUrl := pull["html_url"]
			org := strings.Split(htmlUrl.(string), "/")[3]
			if org != "src-openeuler" && org != "openeuler" {
				continue
			}
			repo := org + "/" + strings.Split(htmlUrl.(string), "/")[4]
			author := pull["user"].(map[string]interface{})["login"]
			draft := pull["draft"]
			mergeAble := pull["mergeable"]
			if draft == true {
				status = UpdatePullsStatus(status, "草稿")
			}
			if mergeAble == false {
				status = UpdatePullsStatus(status, "存在冲突")
			}
			createdAt := pull["created_at"]
			sig := GetSigByRepo(repos, repo)
			labels := pull["labels"]
			tags := make([]string, 0)
			if labels != nil {
				for _, label := range labels.([]interface{}) {
					tags = append(tags, label.(map[string]interface{})["name"].(string))
				}
			}
			pullExists := SearchPull(db, htmlUrl.(string))
			if pullExists == true {
				fmt.Println("PR已存在，更新记录：", htmlUrl)
				updateSql := fmt.Sprintf("update pull set sig='%s', status='%s' where link='%s'", sig, GetPullStatus(tags, status), htmlUrl)
				result, err := db.Exec(updateSql)
				if err != nil {
					fmt.Println("Update failed:", err, ", sql:", updateSql)
				}
				id, err := result.RowsAffected()
				if err != nil {
					fmt.Println("RowsAffected failed:", err)
				}
				fmt.Println("Update successfully:", id)
			} else {
				fmt.Println("PR不存在，插入记录：", htmlUrl)
				r, err := db.Exec("insert into pull(sig, link, status, author, createdAt)values(?, ?, ?, ?, ?)", sig, htmlUrl, GetPullStatus(tags, status), author, createdAt)
				if err != nil {
					fmt.Println("exec failed, ", err)
					return
				}
				id, err := r.LastInsertId()
				if err != nil {
					fmt.Println("exec failed, ", err)
					return
				}
				fmt.Println("insert successfully:", id)
			}
			pullSingle = append(pullSingle, sig)
			pullSingle = append(pullSingle, htmlUrl.(string))
			createdDays := CountCreatedDays(createdAt.(string))
			pullSingle = append(pullSingle, strconv.FormatInt(createdDays, 10))
			pullSingle = append(pullSingle, GetPullStatus(tags, status))
			pullSingle = append(pullSingle, author.(string))
			pullsAll = append(pullsAll, pullSingle)
		}
		page += 1
	}
	GenerateReportCsv(pullsAll, "pulls")
}

func InitDB(dbHost, dbUser, dbPassword, dbName string, dbPort int64) (db *sql.DB) {
	dataSourceName := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", dbUser, dbPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		fmt.Println("open mysql failed,", err)
		panic(err)
	}
	return
}

func SearchPull(db *sql.DB, htmlUrl string) bool {
	row := db.QueryRow("select * from pull where link=?", htmlUrl)
	var id int
	var sig, link, status, author, createdAt string
	switch err := row.Scan(&id, &sig, &link, &status, &author, &createdAt); err {
	case sql.ErrNoRows:
		return false
	case nil:
		return true
	default:
		panic(err)
	}
}

func SearchIssue(db *sql.DB, htmlUrl string) bool {
	row := db.QueryRow("select * from issue where link=?", htmlUrl)
	var id int
	var sig, repo, link, number, issueType, issueState, author, createdAt string
	switch err := row.Scan(&id, &sig, &repo, &link, &number, &issueType, &issueState, &author, &createdAt); err {
	case sql.ErrNoRows:
		return false
	case nil:
		return true
	default:
		panic(err)
	}
}

func main() {
	flag.Parse()
	db := InitDB(*dbHost, *dbUser, *dbPassword, *dbName, *dbPort)
	if *t == "" {
		// Inputting "-t" on the command line is required
		fmt.Println("token is required to execute the script.")
		return
	}
	if *i != true {
		// Default to get pulls statistics
		fmt.Println("Starting to get statistics of pulls.")
		UpdateEnterprisePulls(*t, db)
	} else {
		// Get issues statistics when adding "-i" on the command line
		fmt.Println("Starting to get statistics of issues.")
		UpdateEnterpriseIssues(*t, db)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("Fail to close DB, err:", err)
		}
	}(db)
}
