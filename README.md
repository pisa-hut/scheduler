# PISA Scheduler

Polls the manager for pending tasks and submits SLURM jobs to execute them.

Runs on the SLURM login node as a long-running process.

## Setup

```bash
git clone --recurse-submodules git@github.com:pisa-hut/schedular.git
cd schedular
go build -o scheduler .

# Set up executor dependencies
cd executor
cp .env.example .env
uv sync
cd ..

# Configure scheduler
cp .env.example .env
# Edit .env with your values
./scheduler
```

## Flow

1. Scheduler polls `GET /manager/task` for pending tasks
2. For each new pending task, submits a SLURM job via `sbatch`
3. The SLURM job runs the executor (`uv run python -m executor.main`)
4. The executor claims the task, runs it, and reports the result
5. Scheduler cleans up its tracking when tasks leave pending state

## Configuration

See `.env.example` for all options. Key settings:

- `MANAGER_URL` - Manager API endpoint
- `EXECUTOR_DIR` - Path to the executor project (where `uv run` works)
- `POLL_INTERVAL` - How often to check for new tasks (seconds)
- `MAX_CONCURRENT_JOBS` - Limit concurrent SLURM jobs (0 = unlimited)
- `EXECUTOR_BACKEND` - `apptainer` (production) or `docker` (dev)
- `FILTER_*` - Optional filters for AV, simulator, map, sampler
