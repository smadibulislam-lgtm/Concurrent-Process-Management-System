
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_PROCESSES 64
#define MAX_CHILDREN 64
#define MAX_LINE 256
#define SNAPSHOT_TEXT_SIZE 8192
#define SNAPSHOT_QUEUE_SIZE 2048
#define KILL_EXIT_CODE 1

typedef enum
{
    STATE_RUNNING,
    STATE_WAITING,
    STATE_ZOMBIE,
    STATE_REAPED
} ProcessState;

typedef struct
{
    int used;
    int pid;
    int ppid;
    ProcessState state;
    int exit_status;

    int children[MAX_CHILDREN];
    int child_count;
    pthread_mutex_t wait_mutex;
    pthread_cond_t wait_cond;
} PCB;


typedef struct
{
    int thread_number;
    char file_name[256];
} WorkerArg;


typedef struct
{
    char text[SNAPSHOT_TEXT_SIZE];
} SnapshotItem;


PCB process_table[MAX_PROCESSES];
int next_pid = 2;
pthread_mutex_t table_mutex = PTHREAD_MUTEX_INITIALIZER;

SnapshotItem snapshot_queue[SNAPSHOT_QUEUE_SIZE];
int snapshot_head = 0;
int snapshot_tail = 0;
int snapshot_count = 0;

pthread_mutex_t snapshot_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t snapshot_cond = PTHREAD_COND_INITIALIZER;

int workers_done = 0;
FILE *snapshot_file = NULL;


/*Small helper functions*/

void append_text(char *dest, const char *src){
    strncat(dest,src,SNAPSHOT_TEXT_SIZE - strlen(dest) - 1);
}

void append_number_text(char *dest, int value){
    char temp[64];
    sprintf(temp, "%d", value);
    append_text(dest, temp);
}

const char *state_to_text(ProcessState state){
    if (state == STATE_RUNNING)
    {
        return "RUNNING";
    }
    else if (state == STATE_WAITING)
    {
        return "WAITING";
    }
    else if (state == STATE_ZOMBIE)
    {
        return "ZOMBIE";
    }
    else if (state == STATE_REAPED)
    {
        return "REAPED";
    }
    else
    {
        return "UNKNOWN";
    }
}

void trim_newline(char *line)
{
    int len;
    len = strlen(line);
    while (len > 0){
        if (line[len - 1] == '\n' || line[len - 1] == '\r')
        {
            line[len - 1] = '\0';
            len--;
        }
        else
        {
            break;
        }
    }
}


/* Process table helper functions */

int find_process_index_by_pid(int pid){
    int i;
    for (i = 0; i < MAX_PROCESSES; i++){
        if (process_table[i].used == 1)
        {
            if (process_table[i].pid == pid)
            {
                return i;
            }
        }
    }
    return -1;
}

int find_free_slot(){
    int i;
    for (i = 0; i < MAX_PROCESSES; i++){
        if (process_table[i].used == 0)
        {
            return i;
        }
    }
    return -1;
}

void add_child_to_parent(int parent_index, int child_pid){
    if (parent_index < 0)
    {
        return;
    }

    if (process_table[parent_index].child_count < MAX_CHILDREN)
    {
        process_table[parent_index].children[process_table[parent_index].child_count] = child_pid;
        process_table[parent_index].child_count++;
    }
}


void remove_child_from_parent(int parent_index, int child_pid){
    int i;
    int j;
    if (parent_index < 0)
    {
        return;
    }


    for (i = 0; i < process_table[parent_index].child_count; i++){
        if (process_table[parent_index].children[i] == child_pid)
        {
            for (j = i; j < process_table[parent_index].child_count - 1; j++){
                process_table[parent_index].children[j] = process_table[parent_index].children[j + 1];
            }


            process_table[parent_index].children[process_table[parent_index].child_count - 1] = -1;
            process_table[parent_index].child_count--;
            break;
        }
    }
}

int parent_has_any_child(int parent_pid)
{
    int i;
    for (i = 0; i < MAX_PROCESSES; i++){
        if (process_table[i].used == 1)
        {
            if (process_table[i].ppid == parent_pid)
            {
                return 1;
            }
        }
    }
    return 0;
}



int specific_child_exists(int parent_pid, int child_pid){
    int i;
    for (i = 0; i < MAX_PROCESSES; i++)
    {
        if (process_table[i].used == 1)
        {
            if (process_table[i].ppid == parent_pid)
            {
                if (process_table[i].pid == child_pid)
                {
                    return 1;
                }
            }
        }
    }
    return 0;
}

int find_zombie_child_index(int parent_pid, int child_pid){
    int i;

    for (i = 0; i < MAX_PROCESSES; i++){
        if (process_table[i].used == 1)
        {
            if (process_table[i].ppid == parent_pid)
            {
                if (child_pid == -1)
                {
                    if (process_table[i].state == STATE_ZOMBIE)
                    {
                        return i;
                    }
                }
                else
                {
                    if (process_table[i].pid == child_pid)
                    {
                        if (process_table[i].state == STATE_ZOMBIE)
                        {
                            return i;
                        }
                    }
                }
            }
        }
    }
    return -1;
}


/*Snapshot queue functions*/

void enqueue_snapshot_text(const char *text){
    pthread_mutex_lock(&snapshot_mutex);
    if (snapshot_count < SNAPSHOT_QUEUE_SIZE)
    {
        strcpy(snapshot_queue[snapshot_tail].text, text);
        snapshot_tail++;
        if (snapshot_tail == SNAPSHOT_QUEUE_SIZE)
        {
            snapshot_tail = 0;
        }
        snapshot_count++;

        pthread_cond_signal(&snapshot_cond);
    }
    pthread_mutex_unlock(&snapshot_mutex);
}


int dequeue_snapshot_text(char *out_text){
    pthread_mutex_lock(&snapshot_mutex);
    while (snapshot_count == 0 && workers_done == 0){
        pthread_cond_wait(&snapshot_cond, &snapshot_mutex);
    }


    if (snapshot_count == 0 && workers_done == 1){
        pthread_mutex_unlock(&snapshot_mutex);
        return 0;
    }


    strcpy(out_text, snapshot_queue[snapshot_head].text);
    snapshot_head++;
    if (snapshot_head == SNAPSHOT_QUEUE_SIZE){
        snapshot_head = 0;
    }

    snapshot_count--;
    pthread_mutex_unlock(&snapshot_mutex);
    return 1;
}


/* Snapshot build functions */

void build_table_text_locked(char *out_text){
    int i;
    out_text[0] = '\0';
    append_text(out_text, "PID PPID STATE EXIT_STATUS\n");
    append_text(out_text, "----------------------------------------------\n");


    for (i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].used == 1)
        {
            append_number_text(out_text, process_table[i].pid);
            append_text(out_text, " ");

            append_number_text(out_text, process_table[i].ppid);
            append_text(out_text, " ");

            append_text(out_text, state_to_text(process_table[i].state));
            append_text(out_text, " ");

            if (process_table[i].state == STATE_ZOMBIE)
            {
                append_number_text(out_text, process_table[i].exit_status);
            }
            else
            {
                append_text(out_text, "-");
            }

            append_text(out_text, "\n");
        }
    }

    append_text(out_text, "\n");
}



void queue_snapshot_locked(const char *prefix_line){
    char text[SNAPSHOT_TEXT_SIZE];
    char table_text[SNAPSHOT_TEXT_SIZE];
    text[0] = '\0';
    table_text[0] = '\0';

    if (prefix_line != NULL)
    {
        append_text(text, prefix_line);
        append_text(text, "\n");
    }

    build_table_text_locked(table_text);
    append_text(text, table_text);
    enqueue_snapshot_text(text);
}

/*Initialization */

void pm_init(){
    int i;
    int j;
    for (i = 0; i < MAX_PROCESSES; i++)
    {
        process_table[i].used = 0;
        process_table[i].pid = -1;
        process_table[i].ppid = -1;
        process_table[i].state = STATE_REAPED;
        process_table[i].exit_status = 0;
        process_table[i].child_count = 0;

        for (j = 0; j < MAX_CHILDREN; j++)
        {
            process_table[i].children[j] = -1;
        }

        pthread_mutex_init(&process_table[i].wait_mutex, NULL);
        pthread_cond_init(&process_table[i].wait_cond, NULL);
    }

    process_table[0].used = 1;
    process_table[0].pid = 1;
    process_table[0].ppid = 0;
    process_table[0].state = STATE_RUNNING;
    process_table[0].exit_status = 0;
    process_table[0].child_count = 0;
}


void queue_initial_snapshot(){
    pthread_mutex_lock(&table_mutex);
    queue_snapshot_locked("Initial Process Table");
    pthread_mutex_unlock(&table_mutex);
}


/*Internal process actions */

int do_fork_locked(int parent_pid){
    int parent_index;
    int free_index;
    int child_pid;
    parent_index = find_process_index_by_pid(parent_pid);

    if (parent_index == -1)
    {
        return -1;
    }

    free_index = find_free_slot();
    if (free_index == -1)
    {
        return -1;
    }

    child_pid = next_pid;
    next_pid++;

    process_table[free_index].used = 1;
    process_table[free_index].pid = child_pid;
    process_table[free_index].ppid = parent_pid;
    process_table[free_index].state = STATE_RUNNING;
    process_table[free_index].exit_status = 0;
    process_table[free_index].child_count = 0;

    add_child_to_parent(parent_index, child_pid);
    return child_pid;
}



void do_exit_locked(int pid, int status_code){
    int index;
    index = find_process_index_by_pid(pid);
    if (index == -1)
    {
        return;
    }


    if (process_table[index].state == STATE_ZOMBIE)
    {
        return;
    }


    if (process_table[index].state == STATE_REAPED)
    {
        return;
    }

    process_table[index].state = STATE_ZOMBIE;
    process_table[index].exit_status = status_code;
}

void do_reap_locked(int child_index){
    int parent_pid;
    int parent_index;
    int child_pid;
    int j;

    parent_pid = process_table[child_index].ppid;
    child_pid = process_table[child_index].pid;
    parent_index = find_process_index_by_pid(parent_pid);

    if (parent_index != -1)
    {
        remove_child_from_parent(parent_index, child_pid);
    }

    process_table[child_index].state = STATE_REAPED;
    process_table[child_index].used = 0;
    process_table[child_index].pid = -1;
    process_table[child_index].ppid = -1;
    process_table[child_index].exit_status = 0;
    process_table[child_index].child_count = 0;

    for (j = 0; j < MAX_CHILDREN; j++){
        process_table[child_index].children[j] = -1;
    }
}


/*Public process manager functions */

int pm_fork(int parent_pid){
    int result;
    pthread_mutex_lock(&table_mutex);
    result = do_fork_locked(parent_pid);

    pthread_mutex_unlock(&table_mutex);
    return result;
}

void pm_exit_process(int pid, int status_code){
    int parent_pid;
    int index;
    int parent_index;

    parent_pid = -1;
    pthread_mutex_lock(&table_mutex);
    index = find_process_index_by_pid(pid);

    if (index != -1)
    {
        parent_pid = process_table[index].ppid;
        do_exit_locked(pid, status_code);
    }


    pthread_mutex_unlock(&table_mutex);


    if (parent_pid != -1)
    {
        parent_index = find_process_index_by_pid(parent_pid);

        if (parent_index != -1)
        {
            pthread_mutex_lock(&process_table[parent_index].wait_mutex);
            pthread_cond_broadcast(&process_table[parent_index].wait_cond);
            pthread_mutex_unlock(&process_table[parent_index].wait_mutex);
        }
    }
}



void pm_kill(int pid)
{
    pm_exit_process(pid, KILL_EXIT_CODE);
}



int pm_wait(int parent_pid, int child_pid, int *returned_status)
{
    int parent_index;
    int zombie_index;
    pthread_mutex_t *parent_wait_mutex;
    *returned_status = 0;


    pthread_mutex_lock(&table_mutex);


    parent_index = find_process_index_by_pid(parent_pid);


    if (parent_index == -1)
    {
        pthread_mutex_unlock(&table_mutex);
        return 0;
    }



    if (child_pid == -1)
    {
        if (parent_has_any_child(parent_pid) == 0)
        {
            pthread_mutex_unlock(&table_mutex);
            return 0;
        }
    }
    else
    {
        if (specific_child_exists(parent_pid, child_pid) == 0)
        {
            pthread_mutex_unlock(&table_mutex);
            return 0;
        }
    }


    zombie_index = find_zombie_child_index(parent_pid, child_pid);


    if (zombie_index != -1){
        *returned_status = process_table[zombie_index].exit_status;
        pthread_mutex_unlock(&table_mutex);
        return 1;
    }

    process_table[parent_index].state = STATE_WAITING;

    parent_wait_mutex = &process_table[parent_index].wait_mutex;
    pthread_mutex_unlock(&table_mutex);
    pthread_mutex_lock(parent_wait_mutex);



    while (1){
        pthread_cond_wait(&process_table[parent_index].wait_cond,
                          parent_wait_mutex);

        pthread_mutex_lock(&table_mutex);

        parent_index = find_process_index_by_pid(parent_pid);

        if (parent_index == -1)
        {
            pthread_mutex_unlock(&table_mutex);
            pthread_mutex_unlock(parent_wait_mutex);
            return 0;
        }


        zombie_index = find_zombie_child_index(parent_pid, child_pid);



        if (zombie_index != -1)
        {
            *returned_status = process_table[zombie_index].exit_status;

            process_table[parent_index].state = STATE_RUNNING;


            pthread_mutex_unlock(&table_mutex);
            break;
        }



        if (child_pid == -1)
        {
            if (parent_has_any_child(parent_pid) == 0)
            {
                process_table[parent_index].state = STATE_RUNNING;
                pthread_mutex_unlock(&table_mutex);
                break;
            }
        }
        else
        {
            if (specific_child_exists(parent_pid, child_pid) == 0)
            {
                process_table[parent_index].state = STATE_RUNNING;
                pthread_mutex_unlock(&table_mutex);
                break;
            }
        }

        pthread_mutex_unlock(&table_mutex);
    }

    pthread_mutex_unlock(parent_wait_mutex);
    return 1;
}



void pm_ps(){
    char text[SNAPSHOT_TEXT_SIZE];
    pthread_mutex_lock(&table_mutex);
    build_table_text_locked(text);
    pthread_mutex_unlock(&table_mutex);
    printf("%s", text);
}



/* Worker wrapper functions */

int worker_call_fork(int thread_number, int parent_pid){
    int result;
    char action_line[128];
    pthread_mutex_lock(&table_mutex);
    result = do_fork_locked(parent_pid);


    if (result != -1){
        sprintf(action_line,
                "Thread %d calls pm_fork %d",thread_number,parent_pid);
        queue_snapshot_locked(action_line);
    }

    pthread_mutex_unlock(&table_mutex);
    return result;
}



void wake_parent_if_waiting(int parent_pid){
    int parent_index;
    parent_index = find_process_index_by_pid(parent_pid);
    if (parent_index == -1)
    {
        return;
    }

    pthread_mutex_lock(&process_table[parent_index].wait_mutex);
    pthread_cond_broadcast(&process_table[parent_index].wait_cond);
    pthread_mutex_unlock(&process_table[parent_index].wait_mutex);
}



void worker_call_exit(int thread_number, int pid, int status_code){
    int index;
    int parent_pid;
    char action_line[128];
    parent_pid = -1;


    pthread_mutex_lock(&table_mutex);
    index = find_process_index_by_pid(pid);


    if (index != -1){
        parent_pid = process_table[index].ppid;
        do_exit_locked(pid, status_code);
        sprintf(action_line,
                "Thread %d calls pm_exit %d %d",thread_number,pid,status_code);

        queue_snapshot_locked(action_line);
    }

    pthread_mutex_unlock(&table_mutex);

    if (parent_pid != -1)
    {
        wake_parent_if_waiting(parent_pid);
    }
}



void worker_call_kill(int thread_number, int pid){
    int index;
    int parent_pid;

    char action_line[128];
    parent_pid = -1;

    pthread_mutex_lock(&table_mutex);
    index = find_process_index_by_pid(pid);


    if (index != -1)
    {
        parent_pid = process_table[index].ppid;

        do_exit_locked(pid, KILL_EXIT_CODE);
        sprintf(action_line,"Thread %d calls pm_kill %d",thread_number,pid);
        queue_snapshot_locked(action_line);
    }

    pthread_mutex_unlock(&table_mutex);

    if (parent_pid != -1)
    {
        wake_parent_if_waiting(parent_pid);
    }
}

int worker_call_wait(int thread_number,int parent_pid,int child_pid,int *returned_status){
    int parent_index;
    int zombie_index;
    pthread_mutex_t *parent_wait_mutex;

    char action_line[128];
    *returned_status = 0;

    pthread_mutex_lock(&table_mutex);
    parent_index = find_process_index_by_pid(parent_pid);


    if (parent_index == -1)
    {
        pthread_mutex_unlock(&table_mutex);
        return 0;
    }

    if (child_pid == -1)
    {
        if (parent_has_any_child(parent_pid) == 0)
        {
            pthread_mutex_unlock(&table_mutex);
            return 0;
        }
    }
    else
    {
        if (specific_child_exists(parent_pid, child_pid) == 0)
        {
            pthread_mutex_unlock(&table_mutex);
            return 0;
        }
    }

    zombie_index = find_zombie_child_index(parent_pid, child_pid);

    if (zombie_index != -1)
    {
        *returned_status = process_table[zombie_index].exit_status;


        sprintf(action_line,"Thread %d calls pm_wait %d %d",thread_number,parent_pid,child_pid);
        queue_snapshot_locked(action_line);
        pthread_mutex_unlock(&table_mutex);
        return 1;
    }

    process_table[parent_index].state = STATE_WAITING;
    sprintf(action_line,"Thread %d calls pm_wait %d %d",thread_number,parent_pid,child_pid);
    queue_snapshot_locked(action_line);

    parent_wait_mutex = &process_table[parent_index].wait_mutex;
    pthread_mutex_unlock(&table_mutex);
    pthread_mutex_lock(parent_wait_mutex);


    while (1){
        pthread_cond_wait(&process_table[parent_index].wait_cond,parent_wait_mutex);
        pthread_mutex_lock(&table_mutex);

        parent_index = find_process_index_by_pid(parent_pid);
        if (parent_index == -1)
        {
            pthread_mutex_unlock(&table_mutex);
            pthread_mutex_unlock(parent_wait_mutex);
            return 0;
        }


        zombie_index = find_zombie_child_index(parent_pid, child_pid);



        if (zombie_index != -1)
        {
            *returned_status = process_table[zombie_index].exit_status;
            process_table[parent_index].state = STATE_RUNNING;

            queue_snapshot_locked(NULL);
            pthread_mutex_unlock(&table_mutex);
            break;
        }

        if (child_pid == -1)
        {
            if (parent_has_any_child(parent_pid) == 0)
            {
                process_table[parent_index].state = STATE_RUNNING;

                queue_snapshot_locked(NULL);
                pthread_mutex_unlock(&table_mutex);

                break;
            }
        }

        else
        {
            if (specific_child_exists(parent_pid, child_pid) == 0)
            {
                process_table[parent_index].state = STATE_RUNNING;

                queue_snapshot_locked(NULL);

                pthread_mutex_unlock(&table_mutex);
                break;
            }
        }
        pthread_mutex_unlock(&table_mutex);
    }
    pthread_mutex_unlock(parent_wait_mutex);


    return 1;
}


/*Monitor thread */

void *monitor_thread_function(void *arg){

    char text[SNAPSHOT_TEXT_SIZE]; 
        (void)arg; 
    while (1){
         if (dequeue_snapshot_text(text) == 0) 
         { 
            break;
         }     
        fprintf(snapshot_file, "%s", text); 
        fflush(snapshot_file);
     } 
    return NULL;
}


/*Worker thread */

void *worker_thread_function(void *arg){
    WorkerArg *worker_arg;
    FILE *fp;

    char line[MAX_LINE];
    worker_arg = (WorkerArg *)arg;

    fp = fopen(worker_arg->file_name, "r");



    if (fp == NULL)
    {
        printf("Could not open file %s\n", worker_arg->file_name);
        return NULL;
    }

    while (fgets(line, sizeof(line), fp) != NULL){
        char command[64];
        trim_newline(line);


        if (strlen(line) == 0)
        {
            continue;
        }

        if (sscanf(line, "%s", command) != 1)
        {
            continue;
        }

        if (strcmp(command, "fork") == 0)
        {
            int parent_pid;

            if (sscanf(line, "fork %d", &parent_pid) == 1)
            {
                worker_call_fork(worker_arg->thread_number, parent_pid);
            }
        }
        else if (strcmp(command, "exit") == 0)
        {
            int pid;
            int status_number;

            if (sscanf(line, "exit %d %d", &pid, &status_number) == 2)
            {
                worker_call_exit(worker_arg->thread_number,pid,status_number);
            }
        }


        else if (strcmp(command, "wait") == 0)
        {
            int parent_pid;
            int child_pid;
            int returned_status;

            if (sscanf(line, "wait %d %d", &parent_pid, &child_pid) == 2)
            {
                worker_call_wait(worker_arg->thread_number,parent_pid,child_pid,&returned_status);
            }
        }
        else if (strcmp(command, "kill") == 0)
        {
            int pid;

            if (sscanf(line, "kill %d", &pid) == 1)
            {
                worker_call_kill(worker_arg->thread_number, pid);
            }
        }
        else if (strcmp(command, "sleep") == 0)
        {
            int milliseconds;

            if (sscanf(line, "sleep %d", &milliseconds) == 1)
            {
                usleep(milliseconds * 1000);
            }
        }
        else if (strcmp(command, "ps") == 0)
        {
            pm_ps();
        }
    }

    fclose(fp);

    return NULL;
}





/* Main */



int main(int argc, char *argv[])
{
    pthread_t monitor_thread;
    pthread_t *worker_threads;
    WorkerArg *worker_args;

    int worker_count;
    int i;

    if (argc < 2)
    {
        printf("Usage: %s thread0.txt thread1.txt thread2.txt\n", argv[0]);
        return 1;
    }

    snapshot_file = fopen("snapshots.txt", "w");

    if (snapshot_file == NULL)
    {
        printf("Could not open snapshots.txt\n");
        return 1;
    }

    pm_init();

    worker_count = argc - 1;

    worker_threads = (pthread_t *)malloc(sizeof(pthread_t) * worker_count);
    worker_args = (WorkerArg *)malloc(sizeof(WorkerArg) * worker_count);

    pthread_create(&monitor_thread, NULL, monitor_thread_function, NULL);

    queue_initial_snapshot();

    for (i = 0; i < worker_count; i++)
    {
        worker_args[i].thread_number = i;
        strcpy(worker_args[i].file_name, argv[i + 1]);

        pthread_create(&worker_threads[i], NULL, worker_thread_function, &worker_args[i]);
    }

    for (i = 0; i < worker_count; i++)
    {
        pthread_join(worker_threads[i], NULL);
    }

    pthread_mutex_lock(&snapshot_mutex);
    workers_done = 1;
    pthread_cond_signal(&snapshot_cond);
    pthread_mutex_unlock(&snapshot_mutex);

    pthread_join(monitor_thread, NULL);

    fclose(snapshot_file);

    free(worker_threads);
    free(worker_args);

    return 0;
}