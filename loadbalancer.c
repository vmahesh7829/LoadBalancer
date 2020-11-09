#include<err.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<unistd.h>
#include <stdbool.h>
#include <errno.h>
#include <limits.h> 
#include <pthread.h> 
#include <time.h>
#include <semaphore.h>

#define BUFFER_SIZE 4096

struct server_information {
    bool half_read;        
    int content_length;   
    int errors; 
    int total_requests;
    int fd;
    int server_num;
    int port_num;
    bool answered;
    bool server_failed;
};

struct Queue 
{ 
    int front, rear; 
    unsigned capacity, size; 
    int* array; 
} *queue_ptr; 

int queue_size;
int global_server;
int num_ports;

pthread_mutex_t mutex;
pthread_cond_t cond;

sem_t empty; 
sem_t full;

pthread_cond_t      h_cond  = PTHREAD_COND_INITIALIZER;
pthread_mutex_t     h_mutex = PTHREAD_MUTEX_INITIALIZER;

struct server_information *global_serv_info;

#define WAIT_TIME_SECONDS       4

void print_current_time(){
  time_t rawtime;
  struct tm * timeinfo;

  time ( &rawtime );
  timeinfo = localtime ( &rawtime );
  printf ( "Current local time and date: %s", asctime (timeinfo) );

}

void *h_func(void *arr)
{
  int               rc;
  struct timespec   ts;
  struct timeval    tp;

  int* server_list = (int* )(arr);

  

  /* Usually worker threads will loop on these operations */
  while (1) {
    rc =  gettimeofday(&tp, NULL);
    //printf("time of day is: %d\n", rc);

    /* Convert from timeval to timespec */
    ts.tv_sec  = tp.tv_sec;
    ts.tv_nsec = tp.tv_usec * 1000;
    ts.tv_sec += WAIT_TIME_SECONDS;

    
    pthread_mutex_lock(&h_mutex);
    rc = pthread_cond_timedwait(&h_cond, &h_mutex, &ts);
    //print_current_time();

    
    get_healthcheck_response(server_list,global_serv_info);
    pthread_mutex_unlock(&h_mutex); 
  }

  return NULL;
}



  
// function to create a queue of given capacity.  
// It initializes size of queue as 0 
struct Queue* createQueue(unsigned capacity) 
{ 
    struct Queue* queue = (struct Queue*) malloc(sizeof(struct Queue)); 
    queue->capacity = capacity; 
    queue->front = queue->size = 0;  
    queue->rear = capacity - 1;  // This is important, see the enqueue 
    queue->array = (int*) malloc(queue->capacity * sizeof(int)); 
    return queue; 
} 
  
// Queue is full when size becomes equal to the capacity  
int isFull(struct Queue* queue) 
{  return (queue->size == queue->capacity);  } 
  
// Queue is empty when size is 0 
int isEmpty(struct Queue* queue) 
{  return (queue->size == 0); } 
  
// Function to add an item to the queue.   
// It changes rear and size 
void enqueue(struct Queue* queue, int item) 
{ 
    if (isFull(queue)) 
        return; 
    queue->rear = (queue->rear + 1)%queue->capacity; 
    queue->array[queue->rear] = item; 
    queue->size = queue->size + 1; 
   // printf("%d enqueued to queue\n", item); 
} 
  
// Function to remove an item from queue.  
// It changes front and size 
int dequeue(struct Queue* queue) 
{ 
    if (isEmpty(queue)) 
        return INT_MIN; 
    int item = queue->array[queue->front]; 
    queue->front = (queue->front + 1)%queue->capacity; 
    queue->size = queue->size - 1; 
    return item; 
} 
  
// Function to get front of queue 
int front(struct Queue* queue) 
{ 
    if (isEmpty(queue)) 
        return INT_MIN; 
    return queue->array[queue->front]; 
} 
  
// Function to get rear of queue 
int rear(struct Queue* queue) 
{ 
    if (isEmpty(queue)) 
        return INT_MIN; 
    return queue->array[queue->rear]; 
} 

/*
 * client_connect takes a port number and establishes a connection as a client.
 * connectport: port number of server to connect to
 * returns: valid socket if successful, -1 otherwise
 */
int client_connect(uint16_t connectport) {
    int connfd;
    struct sockaddr_in servaddr;

    connfd=socket(AF_INET,SOCK_STREAM,0);
    if (connfd < 0)
        return -1;
    memset(&servaddr, 0, sizeof servaddr);

    servaddr.sin_family=AF_INET;
    servaddr.sin_port=htons(connectport);

    /* For this assignment the IP address can be fixed */
    inet_pton(AF_INET,"127.0.0.1",&(servaddr.sin_addr));

    if(connect(connfd,(struct sockaddr *)&servaddr,sizeof(servaddr)) < 0)
        return -1;
    return connfd;
}

/*
 * server_listen takes a port number and creates a socket to listen on 
 * that port.
 * port: the port number to receive connections
 * returns: valid socket if successful, -1 otherwise
 */
int server_listen(int port) {
    int listenfd;
    int enable = 1;
    struct sockaddr_in servaddr;

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0)
        return -1;
    memset(&servaddr, 0, sizeof servaddr);
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htons(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if(setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0)
        return -1;
    if (bind(listenfd, (struct sockaddr*) &servaddr, sizeof servaddr) < 0)
        return -1;
    if (listen(listenfd, 500) < 0)
        return -1;
    return listenfd;
}

/*
 * bridge_connections send up to 100 bytes from fromfd to tofd
 * fromfd, tofd: valid sockets
 * returns: number of bytes sent, 0 if connection closed, -1 on error
 */
int bridge_connections(int fromfd, int tofd) {

   // printf("begin bridge connection call: \n");

    char recvline[BUFFER_SIZE +1];
    int n = recv(fromfd, recvline, BUFFER_SIZE, 0);
    if (n < 0) {
     //   printf("connection error receiving\n");
        return -1;
    } else if (n == 0) {
      //  printf("receiving connection ended\n");
        return 0;
    }
    recvline[n] = '\0';
   // printf("recvline is: %s\n", recvline);
    n = send(tofd, recvline, n, 0);
    if (n < 0) {
     //   printf("connection error sending\n");
        return -1;
    } else if (n == 0) {
    //    printf("sending connection ended\n");
        return 0;
    }
    return n;
}



int parse_response(struct server_information* serv_info){

    char str_out[1000];
    memset(&str_out, 0, sizeof(str_out) );


    if (serv_info->half_read == false){


        int n = recv(serv_info->fd, str_out, BUFFER_SIZE, 0);
        printf("str_out is: %s\n", str_out);
        printf("n is: %d\n",n);

         if(n == 0){

            serv_info->server_failed = true;
            return 1;
        } 

        char* rest = str_out; 
        char* token = strtok_r(rest, " ", &rest);


        token = strtok_r(rest, "\r\n", &rest);

        if(strcmp(token, "200 OK") != 0 ){
            serv_info->server_failed = true;
            return 1;
        }

        token = strtok_r(rest, " ", &rest);
        token = strtok_r(rest, "\r\n", &rest);

    
    if( strlen(rest) == 3 ){
        serv_info->half_read = true;
        serv_info->content_length = atoi(token);
        return 0;
    }

    token = strtok_r(rest, "\n", &rest);
    token = strtok_r(rest, "\n", &rest);


    serv_info->errors = atoi(token);
    serv_info->total_requests = atoi(rest);


    return 1;

    }
    else{

        int n = recv(serv_info->fd, str_out, BUFFER_SIZE, 0);

      //  printf("str_out is: %s\n", str_out);

        char* rest = str_out; 
        char* token = strtok_r(rest, "\n", &rest);
        serv_info->errors = atoi(token);
        serv_info->total_requests = atoi(rest);


        return 1;
    }

}

// this should be called pick server
// put bool server success or server fail into the struct and only look at successful servers

   

int pick_server(struct server_information* server_info){
    int smallest_server;
    int least_requests = 2147483647;
    bool tiebreak = false;
    int least_errors;
    int server_with_least_errors;

    // if a healthy server has not been found throw a 500 error(by returning -2)
    // this should happen if all servers are down initially
    // this should happen if all servers are non-responsive

    // look through all the servers and find the one with the least requests
    // that one is set as the smallest server

    bool all_failed = true;
    

    for (int i = 0; i<num_ports; i++){

  //      printf("i = %d\n", i);
        if(server_info[i].server_failed != true ){
            all_failed = false;
            if (server_info[i].total_requests < least_requests){
                smallest_server = i;
                server_with_least_errors = i;
                least_requests = server_info[i].total_requests;
                least_errors = server_info[i].errors;
                tiebreak = false;
            }
            else {
                if( server_info[i].total_requests == least_requests  ){
                    tiebreak = true;
                    if( server_info[i].errors < least_errors  ){
                        least_errors = server_info[i].errors;
                        server_with_least_errors = i;
                    }
                }  
            }
        }    
 }

    if(all_failed == true){
        return -1;
    }

    if(tiebreak == true){
        return server_with_least_errors;
    }
    else{
        return smallest_server;
    } 


}



// poll_healthcheck takes in an array of server file descriptors.
// it sends a healthcheck request: 

int get_healthcheck_response(int *server_port, struct server_information* server_info ){

    // this holds info for each server
    char str_out[1000];
    memset(&str_out, 0, sizeof(str_out) );

    //printf("num ports is: %d\n", num_ports);

    int health_response;
    //servers not responded tracks how many servers the function is still
    // expecting a healthcheck from
    // it is decremented when:
    // a valid response is received
    // server doesn't respond fast enough (timeout)
    // a non-200 healthcheck is received
    // a server is down from the start

    int servers_not_responded = num_ports;

    //create a socket for each server and send a healthcheck request
    for (int i = 0; i<num_ports; i++){
        //printf("server port i is: %d\n", server_port[i]);

        server_info[i].fd = client_connect(server_port[i]);
        printf("server %d has fd %d\n", i, server_info[i].fd );

        server_info[i].server_num = i;
        server_info[i].half_read = false;
        server_info[i].server_failed = false;
        server_info[i].answered = false;
        server_info[i].port_num = server_port[i];
        
        if(server_info[i].fd == -1 ){
            server_info[i].server_failed = true;
            servers_not_responded -=1;
        }
        else{
            sprintf(str_out, "GET /healthcheck HTTP/1.1\r\n\r\n");
            health_response = write(server_info[i].fd,str_out,strlen(str_out));
          //  printf("writing health response\n");
            memset(&str_out, 0, sizeof(str_out) );
        }
    }

  

    fd_set master_set, working_set;
    int num_responses;
    struct timeval timeout;
    bool received_answer = false;

    FD_ZERO(&master_set);

    // add all non failed servers that are up to master set
    for (int i=0; i<num_ports; i++){
        if (server_info[i].server_failed == false ){
             FD_SET (server_info[i].fd, &master_set);
        }
    }

    timeout.tv_sec = 2;
    timeout.tv_usec = 0;

    int client_done;

    while(servers_not_responded > 0 ){
        //printf("servers not responded is: %d\n", servers_not_responded);
        
        memcpy(&working_set, &master_set, sizeof(master_set));
     //   printf("waiting for a response\n");
    //    printf("FDSETSIZE is: %d\n", FD_SETSIZE);
        num_responses = select(FD_SETSIZE, &working_set, NULL, NULL, &timeout);
        //printf("num_responses are: %d\n", num_responses);

        if(num_responses == -1){
     printf("error during healthcheck, exiting\n");
            for (int i=0; i<num_ports;i++){
                if(server_info[i].answered == 0 ){
                    server_info[i].server_failed = true;
                    close(server_info[i].fd);
                    FD_CLR(server_info[i].fd, &master_set);
                }
            return;
            }
        }
        else if (num_responses == 0){
        printf("timeout hit during server selection\n");
            for (int i=0; i<num_ports;i++){
                if(server_info[i].answered == false ){
                    server_info[i].server_failed = true;
                    close(server_info[i].fd);
                    FD_CLR(server_info[i].fd, &master_set);
                    servers_not_responded -= 1;
                }
            }
        }
        else{
            for (int i=0; i<num_ports && num_responses>0; i++){
            //printf("i is: %d\n",i);
                if (FD_ISSET(server_info[i].fd, &working_set)) {
                printf("going into server %d\n", i);
				client_done = parse_response(&server_info[i]);
                servers_not_responded -= client_done;

                if(client_done == 1){
                    //printf("closing fd %d\n",server_info[i].fd );
                    server_info[i].answered = true;
                    received_answer = true;
                    close(server_info[i].fd);
                    FD_CLR(server_info[i].fd, &master_set);
                }

               num_responses -= 1;
			}
         }
      }
    }
    return;

} 

/*
 * bridge_loop forwards all messages between both sockets until the connection
 * is interrupted. It also prints a message if both channels are idle.
 * sockfd1, sockfd2: valid sockets
 */
void bridge_loop(int client_fd, int server_fd, int server_no, struct server_information* server_info ) {
    fd_set set;
    struct timeval timeout;
    bool received_something = false;



    int fromfd, tofd;
    while(1) {
        // set for select usage must be initialized before each select call
        // set manages which file descriptors are being watched
        FD_ZERO (&set);
        FD_SET (client_fd, &set);
        FD_SET (server_fd, &set);

        // same for timeout
        // max time waiting, 4 seconds, 0 microseconds
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;

        // select return the number of file descriptors ready for reading in set
        switch (select(FD_SETSIZE, &set, NULL, NULL, &timeout)) {
            case -1:
                return;
            case 0:
                if (received_something == false){
                    char str_out[1000];
                    memset(&str_out, 0, sizeof(str_out) );
                    sprintf(str_out, "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");

                    write(client_fd,str_out,strlen(str_out));

                    server_info[server_no].server_failed = true;

                    return;
                }
            default:
                if (FD_ISSET(client_fd, &set)) {
                    fromfd = client_fd;
                    tofd = server_fd;
                } else if (FD_ISSET(server_fd, &set)) {
                    fromfd = server_fd;
                    tofd = client_fd;
                } else {
                    return;
                }
        }
        if (bridge_connections(fromfd, tofd) <= 0){

            if (fromfd == server_fd){
                received_something = true;
            }
            server_info[server_no].total_requests += 1;
            return;
        }
            
    }
}

static void *worker_thread(void *arg)
{

    int worker_fd;
    int accept_fd;
    int server_picked;
    int server_port;
    int conn_fd;

    char str_out[100];

    while(1) {

        sem_wait(&full); 
        pthread_mutex_lock(&mutex);
    
        //Extract work from queue
        queue_size -= 1;
        accept_fd = dequeue(queue_ptr);
        pthread_mutex_unlock(&mutex);
        sem_post(&empty); 

        
        // find the best server

        pthread_mutex_lock(&h_mutex);
        server_picked = pick_server(global_serv_info);
        server_port = global_serv_info[server_picked].port_num ;
        conn_fd = client_connect(server_port);
        printf("server_picked is: %d\n", server_picked);
        printf("conn_fd is: %d\n", conn_fd);
        
        if(server_picked == -1){
            memset(&str_out, 0, sizeof(str_out) );
            sprintf(str_out, "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
            write(accept_fd,str_out,strlen(str_out));
            global_serv_info[server_picked].server_failed = true;
            close(accept_fd);
            close(conn_fd);
       }
       else{
            bridge_loop(accept_fd, conn_fd,server_picked,global_serv_info );
       }
       pthread_mutex_unlock(&h_mutex);



    }
    return;
}



int main(int argc,char **argv) {
    int connfd, listenfd, acceptfd;
    uint16_t connectport, listenport;

    if (argc < 3) {
        printf("missing arguments: usage %s port_to_connect port_to_listen", argv[0]);
        return 1;
    }

    bool r_flag = false;
    bool n_flag = false;

    int nthreads = 4;
    int r_val = 5 ;

    char* port_number = 8080;

    int index;
    int c;

    opterr = 0;

    while ((c = getopt(argc, argv, "R:N:")) != -1) {
        switch (c) {
            case 'R':
                r_flag = true;
                r_val = atoi(optarg);

                break;
            case 'N':
                n_flag = true;
                nthreads = atoi(optarg);

                break;
            
        }
    }


    int arr_size= argc - optind -1;
    int server_list[arr_size];

    listenport = atoi(argv[optind]);

    for (index = optind+1; index < argc; index++) {
        port_number = argv[index];   
        server_list[index-optind -1 ] = atoi(port_number);
    }

    global_serv_info = malloc( sizeof(struct server_information)*arr_size);


    //Args are parsed before here

    // listen
    if ((listenfd = server_listen(listenport)) < 0)
        err(1, "failed listening");

    
    int queue_cap = 100;


    // initialize the queue here make it a queue that holds a struct first.
    // pass both the client fd and the server fd to the queue
    queue_size = 0;
    queue_ptr = createQueue(queue_cap);

    //initialize the threads here
    pthread_t thread[nthreads];

    //create worker threads
    for(int i=0; i<nthreads; i++){
        pthread_create(&(thread[i]), 0, worker_thread, NULL);
    }

    // initialize mutex
    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&cond, 0);

    sem_init(&empty, 0, queue_cap); 
    sem_init(&full, 0, 0);

    num_ports = arr_size;

    pthread_mutex_lock(&h_mutex);
    get_healthcheck_response(server_list,global_serv_info);
    pthread_mutex_unlock(&h_mutex);
    

    pthread_t h_thread;
    pthread_create(&h_thread, NULL, h_func, server_list);
    int requests_handled = 0; 

    

     while (true){

        if (requests_handled%r_val == 0 && requests_handled>0){
            pthread_cond_broadcast(&h_cond);
        }
        
        if ((acceptfd = accept(listenfd, NULL, NULL)) < 0)
        err(1, "failed accepting");


        sem_wait(&empty); 
        pthread_mutex_lock(&mutex);
        //Add work to queue

        queue_size += 1;
        enqueue(queue_ptr,acceptfd);

        pthread_mutex_unlock(&mutex);
        sem_post(&full);

        requests_handled++;
    }
    
    return 0;
    
}
