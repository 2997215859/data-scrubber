package config

import logger "github.com/2997215859/golog"

// 定义全局变量来接收注入的版本信息
var (
	GitCommitSha1    string
	GitCommitDate    string
	GitCommitSubject string
	GitBranchName    string
)

func PrintVersionInfo() {
	logger.Info("")
	logger.Info("============================== Version Info ==============================")
	logger.Info("=== git commit SHA1     :    %s", GitCommitSha1)
	logger.Info("=== git branch name     :    %s", GitCommitDate)
	logger.Info("=== git commit date     :    %s", GitCommitSubject)
	logger.Info("=== git commit subject  :    %s", GitBranchName)
	logger.Info("============================================================================")
	logger.Info("")
}
