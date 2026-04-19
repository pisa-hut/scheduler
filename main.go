package main

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
	"sync"
	"syscall"
	"time"
)

type Task struct {
	ID          int    `json:"id"`
	PlanID      int    `json:"plan_id"`
	AvID        int    `json:"av_id"`
	SimulatorID int    `json:"simulator_id"`
	SamplerID   int    `json:"sampler_id"`
	TaskStatus  string `json:"task_status"`
	CreatedAt   string `json:"created_at"`
	RetryCount  int    `json:"retry_count"`
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

func fetchPendingTasks(cfg Config) ([]Task, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(cfg.ManagerURL + "/task")
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var tasks []Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, fmt.Errorf("decode failed: %w", err)
	}

	var pending []Task
	for _, t := range tasks {
		if t.TaskStatus == "pending" {
			pending = append(pending, t)
		}
	}
	return pending, nil
}

func countSlurmJobs() int {
	out, err := exec.Command("squeue", "--me", "--noheader", "--format=%i").Output()
	if err != nil {
		return 0
	}
	lines := strings.TrimSpace(string(out))
	if lines == "" {
		return 0
	}
	return len(strings.Split(lines, "\n"))
}

func buildSbatchScript(cfg Config, taskID int) string {
	argsStr := fmt.Sprintf("--backend %s --task-id %d", cfg.Backend, taskID)

	partitionLine := ""
	if cfg.SlurmPartition != "" {
		partitionLine = fmt.Sprintf("#SBATCH --partition=%s", cfg.SlurmPartition)
	}

	return fmt.Sprintf(`#!/bin/bash
#SBATCH --job-name=pisa-%d
#SBATCH --output=%s/outputs/stdout/task_%d_%%j
#SBATCH --error=%s/outputs/stderr/task_%d_%%j
#SBATCH --time=%s
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=%s
#SBATCH --mem=%s
%s

cd %s

echo "=== PISA Executor ==="
echo "Task ID:       %d"
echo "Job ID:        $SLURM_JOB_ID"
echo "Node:          $SLURM_NODELIST"
echo "Start Time:    $(date)"
echo

uv run python -m executor.main %s

echo
echo "End Time:      $(date)"
`, taskID,
		executorDir, taskID,
		executorDir, taskID,
		cfg.SlurmTime,
		cfg.SlurmCPUs,
		cfg.SlurmMem,
		partitionLine,
		executorDir,
		taskID,
		argsStr,
	)
}

func submitSlurmJob(cfg Config, taskID int) bool {
	script := buildSbatchScript(cfg, taskID)

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("pisa_task_%d_*.sh", taskID))
	if err != nil {
		log.Printf("[ERROR] Failed to create temp file for task #%d: %v", taskID, err)
		return false
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(script); err != nil {
		log.Printf("[ERROR] Failed to write script for task #%d: %v", taskID, err)
		return false
	}
	tmpFile.Close()

	out, err := exec.Command("sbatch", tmpFile.Name()).CombinedOutput()
	if err != nil {
		log.Printf("[ERROR] sbatch failed for task #%d: %s", taskID, strings.TrimSpace(string(out)))
		return false
	}

	parts := strings.Fields(strings.TrimSpace(string(out)))
	jobID := parts[len(parts)-1]
	log.Printf("[INFO] Submitted SLURM job %s for task #%d", jobID, taskID)
	return true
}

func main() {
	cfg := loadConfig()

	log.Printf("[INFO] PISA Scheduler starting")
	log.Printf("[INFO] Manager URL: %s", cfg.ManagerURL)
	log.Printf("[INFO] Backend: %s", cfg.Backend)
	log.Printf("[INFO] Poll interval: %s", cfg.PollInterval)
	if cfg.MaxJobs > 0 {
		log.Printf("[INFO] Max concurrent jobs: %d", cfg.MaxJobs)
	}

	// Ensure output dirs
	os.MkdirAll(filepath.Join(executorDir, "outputs", "stdout"), 0o755)
	os.MkdirAll(filepath.Join(executorDir, "outputs", "stderr"), 0o755)

	submitted := make(map[int]struct{})
	var mu sync.Mutex

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	poll := func() {
		pending, err := fetchPendingTasks(cfg)
		if err != nil {
			log.Printf("[ERROR] %v", err)
			return
		}

		mu.Lock()
		defer mu.Unlock()

		// Clean up submitted tasks that are no longer pending
		pendingIDs := make(map[int]struct{})
		for _, t := range pending {
			pendingIDs[t.ID] = struct{}{}
		}
		for id := range submitted {
			if _, ok := pendingIDs[id]; !ok {
				delete(submitted, id)
			}
		}

		// Find new tasks
		var newTasks []Task
		for _, t := range pending {
			if _, ok := submitted[t.ID]; !ok {
				newTasks = append(newTasks, t)
			}
		}

		if len(newTasks) == 0 {
			return
		}

		log.Printf("[INFO] Found %d new pending tasks", len(newTasks))

		// Check job limit
		if cfg.MaxJobs > 0 {
			running := countSlurmJobs()
			slots := cfg.MaxJobs - running
			if slots <= 0 {
				log.Printf("[INFO] At max concurrent jobs (%d), skipping", cfg.MaxJobs)
				return
			}
			if len(newTasks) > slots {
				newTasks = newTasks[:slots]
			}
		}

		for _, t := range newTasks {
			if submitSlurmJob(cfg, t.ID) {
				submitted[t.ID] = struct{}{}
			}
		}
	}

	// Initial poll
	poll()

	for {
		select {
		case <-ticker.C:
			poll()
		case <-sig:
			log.Printf("[INFO] Scheduler stopped")
			return
		}
	}
}
