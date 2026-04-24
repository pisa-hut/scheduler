package main

// PISA Scheduler — a capacity controller, intentionally task-blind.
//
// Responsibility: make sure every task that still needs work has (or
// will soon have) an executor to run it.
//
//     queued_slurm_jobs + running_slurm_jobs  ==  pending_tasks + running_tasks
//
// The scheduler never names a task on the sbatch command line; each
// executor, once SLURM places it on a node, calls /task/claim with no
// filter and takes whatever is next. That keeps scheduling and task
// dispatch orthogonal: the manager decides *which* task runs where,
// and in what order (retry, priority, etc.); the scheduler just feeds
// the cluster enough bodies to service the backlog.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Task struct {
	ID         int    `json:"id"`
	TaskStatus string `json:"task_status"`
}

const executorDir = "./executor"

type Config struct {
	ManagerURL     string
	PollInterval   time.Duration
	SlurmPartition string
	SlurmTime      string
	SlurmCPUs      string
	SlurmMem       string
	Backend        string
	MaxJobs        int
	JobName        string
}

func loadConfig() Config {
	loadDotenv()

	pollSec, _ := strconv.Atoi(getenv("POLL_INTERVAL", "10"))
	maxJobs, _ := strconv.Atoi(getenv("MAX_CONCURRENT_JOBS", "0"))

	return Config{
		ManagerURL:     mustEnv("MANAGER_URL"),
		PollInterval:   time.Duration(pollSec) * time.Second,
		SlurmPartition: getenv("SLURM_PARTITION", ""),
		SlurmTime:      getenv("SLURM_TIME", "01:00:00"),
		SlurmCPUs:      getenv("SLURM_CPUS", "12"),
		SlurmMem:       getenv("SLURM_MEM", "12G"),
		Backend:        getenv("EXECUTOR_BACKEND", "apptainer"),
		MaxJobs:        maxJobs,
		JobName:        getenv("SLURM_JOB_NAME", "pisa-exec"),
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("Required environment variable %s is not set", key)
	}
	return v
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func loadDotenv() {
	data, err := os.ReadFile(".env")
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}

// fetchDemand returns the number of tasks that still need an executor
// (queued + running). `running` counts because its executor may die
// without notice; we leave capacity carrying it until the manager's
// reaper marks the task back to `queued`.
func fetchDemand(cfg Config) (int, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(cfg.ManagerURL + "/task")
	if err != nil {
		return 0, fmt.Errorf("fetch tasks: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("task endpoint returned %d", resp.StatusCode)
	}

	var tasks []Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return 0, fmt.Errorf("decode tasks: %w", err)
	}

	count := 0
	for _, t := range tasks {
		if t.TaskStatus == "queued" || t.TaskStatus == "running" {
			count++
		}
	}
	return count, nil
}

// countOurSlurmJobs returns pending+running SLURM jobs owned by the
// current user that match our job name. Anything else in the user's
// queue (other projects, manual submissions) is ignored.
func countOurSlurmJobs(jobName string) int {
	out, err := exec.Command(
		"squeue",
		"--me",
		"--noheader",
		"--name="+jobName,
		"--format=%i",
	).Output()
	if err != nil {
		// squeue missing (e.g. outside SLURM) or transient failure — no
		// visible jobs. We purposely don't return an error so the
		// scheduler keeps trying each tick.
		log.Printf("[WARN] squeue call failed: %v", err)
		return 0
	}
	lines := strings.TrimSpace(string(out))
	if lines == "" {
		return 0
	}
	return len(strings.Split(lines, "\n"))
}

func buildSbatchScript(cfg Config) string {
	argsStr := "--backend " + cfg.Backend

	partitionLine := ""
	if cfg.SlurmPartition != "" {
		partitionLine = fmt.Sprintf("#SBATCH --partition=%s", cfg.SlurmPartition)
	}

	return fmt.Sprintf(`#!/bin/bash
#SBATCH --job-name=%s
#SBATCH --output=%s/outputs/stdout/job_%%j
#SBATCH --error=%s/outputs/stderr/job_%%j
#SBATCH --time=%s
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=%s
#SBATCH --mem=%s
#SBATCH --signal=TERM@60
%s

cd %s

echo "=== PISA Executor ==="
echo "Job ID:     $SLURM_JOB_ID"
echo "Node:       $SLURM_NODELIST"
echo "Start Time: $(date)"
echo

uv run python -m executor.main %s

status=$?
echo
echo "End Time:   $(date)"
echo "Exit:       $status"
exit $status
`, cfg.JobName,
		executorDir,
		executorDir,
		cfg.SlurmTime,
		cfg.SlurmCPUs,
		cfg.SlurmMem,
		partitionLine,
		executorDir,
		argsStr,
	)
}

func submitSlurmJob(cfg Config) bool {
	script := buildSbatchScript(cfg)

	tmpFile, err := os.CreateTemp("", "pisa_exec_*.sh")
	if err != nil {
		log.Printf("[ERROR] temp file: %v", err)
		return false
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(script); err != nil {
		log.Printf("[ERROR] write script: %v", err)
		return false
	}
	tmpFile.Close()

	out, err := exec.Command("sbatch", tmpFile.Name()).CombinedOutput()
	if err != nil {
		log.Printf("[ERROR] sbatch: %s", strings.TrimSpace(string(out)))
		return false
	}

	parts := strings.Fields(strings.TrimSpace(string(out)))
	jobID := parts[len(parts)-1]
	log.Printf("[INFO] submitted SLURM job %s", jobID)
	return true
}

func main() {
	cfg := loadConfig()

	log.Printf("[INFO] PISA Scheduler starting")
	log.Printf("[INFO] manager_url=%s backend=%s job_name=%s", cfg.ManagerURL, cfg.Backend, cfg.JobName)
	log.Printf("[INFO] poll_interval=%s", cfg.PollInterval)
	if cfg.MaxJobs > 0 {
		log.Printf("[INFO] max_concurrent_jobs=%d", cfg.MaxJobs)
	}

	os.MkdirAll(filepath.Join(executorDir, "outputs", "stdout"), 0o755)
	os.MkdirAll(filepath.Join(executorDir, "outputs", "stderr"), 0o755)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	poll := func() {
		demand, err := fetchDemand(cfg)
		if err != nil {
			log.Printf("[ERROR] %v", err)
			return
		}
		supply := countOurSlurmJobs(cfg.JobName)

		toSubmit := demand - supply
		if toSubmit <= 0 {
			if demand > 0 || supply > 0 {
				log.Printf("[INFO] demand=%d supply=%d (balanced)", demand, supply)
			}
			return
		}

		if cfg.MaxJobs > 0 {
			slots := cfg.MaxJobs - supply
			if slots < 0 {
				slots = 0
			}
			if toSubmit > slots {
				toSubmit = slots
			}
		}

		if toSubmit == 0 {
			log.Printf("[INFO] demand=%d supply=%d at max_concurrent_jobs=%d, waiting",
				demand, supply, cfg.MaxJobs)
			return
		}

		log.Printf("[INFO] demand=%d supply=%d → submitting %d", demand, supply, toSubmit)
		for i := 0; i < toSubmit; i++ {
			submitSlurmJob(cfg)
		}
	}

	poll()

	for {
		select {
		case <-ticker.C:
			poll()
		case <-sig:
			log.Printf("[INFO] scheduler stopped")
			return
		}
	}
}
