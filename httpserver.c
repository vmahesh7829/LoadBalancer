#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>


#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <fcntl.h>
#include <string.h> // memset
#include <stdlib.h> // atoi
#include <stdbool.h> // true, false
#include <errno.h>
#include <ctype.h>

#include <limits.h>
#define BUFFER_SIZE 4096
// acknowledgements: queue code is pulled off geeksforgeeks

pthread_mutex_t offset_lock; 

pthread_mutex_t mutex;
pthread_cond_t cond;
extern int errno;

int queue_size;
int client_fd;


int log_offset; 
int log_fd;
bool logging_enabled;

int errors_in_log;
int entries_in_log;




struct httpObject {
    /*
        Create some object 'struct' to keep track of all
        the components related to a HTTP message
        NOTE: There may be more member variables you would want to add
    */
    char method[5];         // PUT, HEAD, GET
    char filename[1024];      // what is the file we are worried about
    char httpversion[9];    // HTTP/1.1
    int content_length; // example: 13
    char status_code[100];
    bool content_length_read;
    int error_number;
};

// A structure to represent a queue 
struct Queue 
{ 
    int front, rear, size; 
    unsigned capacity; 
    int* array; 
} *queue_ptr; 
  
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

bool cont_len_isz (char *c_len){
    if (strcmp(c_len," 0")==0 || strcmp(c_len,"0") ==0 ) {
        return 1;
    }
    else{
        return 0;
    }
}

bool s_code_err (char* status_code){

    if(strlen(status_code)==0){
        return false;
    }
    if (strcmp(status_code,"400 Bad Request") == 0 ){
        return true;
    }
    if (strcmp(status_code,"500 Internal Server Error") ==0){
        return true;
    }
    if (strcmp(status_code,"403 Forbidden") ==0){
        return true;
    }
    return false;
}

bool check_ascii(char input_char) {
    if (isalnum(input_char)!=0 || input_char==45 || input_char ==95){
        return true;
    }
    return false;
}

//this function takes in a buffer
// it returns true if the buffer is <27 legal characters
// else it returns false
bool check_filename(char* buffer){
    if (strlen(buffer)>27){
        return false;
    }
    for(int i=0; i<strlen(buffer); i++){
        if (check_ascii(buffer[i]) == false){
            return false;
            
        }
    }
    return true;
}

bool valid_methodname(struct httpObject* message){
    if (strcmp(message->httpversion,"HTTP/1.1") != 0){
        return false;
    }
    
    else if (strcmp(message->method,"GET") == 0){
        return true;
    }
    else if (strcmp(message->method,"HEAD") == 0){
        return true;
    }
    else if (strcmp(message->method,"PUT") == 0){
        return true;
    }
    else{
        return false;
    }
}


void process_header(char* header, struct httpObject* message){
     // tokenize the header to grab everything before the first :


        char* rest = header;     // this holds everything after :
        char* token = strtok_r(rest, ":", &rest); // this holds everything before :


        if (strcmp(token,"Content-Length") == 0) {
            if (message->content_length_read ==1){
                strcpy(message->status_code,"400 Bad Request");
            }
            else {
                if (cont_len_isz(rest)){
                    message->content_length= 0;
                    message->content_length_read = 1;
                }

                else if (atoi(rest)==0 ){
                    strcpy(message->status_code,"400 Bad Request");
                 }
                else{
                    message->content_length= atoi(rest);
                    message->content_length_read = 1;
                }

            }
        }
        //else (header != content length)
        else{
            if(token==NULL){
                strcpy(message->status_code,"400 Bad Request");
            }
        }
}

 void handle_headers( char* all_headers, struct httpObject* message) {


    // this puts the first header into first_header
     char* rest_headers = all_headers; 
     char* first_header = strtok_r(rest_headers, "\r\n", &rest_headers); // this holds everything before :

    while ( first_header != NULL ) {

        process_header(first_header, message);

        if(strcmp(message->status_code,"400 Bad Request")==0 ){
        return;  // we are still going to return i but it isn't used
      }

      first_header = strtok_r(rest_headers, "\r\n", &rest_headers);
    
    }

}


/*
    \brief 1. Want to read in the HTTP message/ data coming in from socket
    \param client_sockd - socket file descriptor
    \param message - object we want to 'fill in' as we read in the HTTP message
*/
void read_http_request(ssize_t client_sockd, struct httpObject* message) {

    message->content_length_read = 0;
    uint8_t buff[BUFFER_SIZE +1];
    memset(buff, 0, sizeof(buff));

    ssize_t bytes = recv(client_sockd, buff, BUFFER_SIZE,0);
    buff[bytes]= 0;

    char* token; 
    char* rest = buff;

    token = strtok_r(rest, " ", &rest); 
    strcpy(message->method,token);


    token = strtok_r(rest, " /", &rest);
    strcpy(message->filename,token);

    token = strtok_r(rest, "\r\n", &rest);
    strcpy(message->httpversion,token);

    handle_headers(rest,message);

    if(strcmp(message->status_code,"400 Bad Request") ==0){
        return;
    }
    
}

void errcode_respond (int client_sockd, struct httpObject* message, char* str_out ){

    memset(str_out, 0, sizeof(str_out));
    sprintf(str_out, "%s %s \r\nContent-Length: 0\r\n\r\n",message->httpversion,message->status_code);
    write(client_sockd,str_out,strlen(str_out));
    memset(str_out, 0, sizeof(str_out));

    int offset_saver = log_offset;

    if (logging_enabled == true){

    

    //////////////////////log code starts here/////////////////////////////////////////
    sprintf(str_out, "FAIL: %s /%s %s --- response %d\n",message->method,message->filename, message->httpversion,message->error_number);
    sprintf(str_out + strlen(str_out),"========\n");
    

    pthread_mutex_lock(&offset_lock);
    //*****************filelock and increment the buffer offset*********************//
    log_offset += strlen(str_out);
    errors_in_log += 1;
    entries_in_log +=1; 
    
    //*****************END the LOCK *****************//
    pthread_mutex_unlock(&offset_lock);

    pwrite(log_fd, str_out, strlen(str_out), offset_saver);

    /////////////////////////log code ends here////////////////////////////////////////

    }

}

// gen response is a worker function for both 

//REMINDERS:
//this wont work until buffer log_offset is correct.
void gen_response(int in_fd, int out_fd, struct httpObject* message, char* str_out ){

    memset(str_out, 0, sizeof(str_out));

    bool sending_healthcheck = false;
    int nw;

    if (strcmp("healthcheck", message->filename) == 0 ){
        sending_healthcheck = true;
    }

    // if method is get, send the first line of the response to the client
    if(strcmp(message->method,"GET")==0){

        if (sending_healthcheck == false ){
            struct stat fileStat;
            fstat(in_fd, &fileStat);
            message->content_length = fileStat.st_size;
        }
        else{
            sprintf(str_out, "%d\n%d",errors_in_log, entries_in_log );
            message->content_length = strlen(str_out);

        }

        
        memset(str_out, 0, sizeof(str_out));
        //generate status code
        strcpy(message->status_code,"200 OK");

        // send the response to the client but not the file data
        sprintf(str_out, "%s %s \r\nContent-Length: %d\r\n\r\n",message->httpversion,message->status_code,message->content_length);
        write(out_fd,str_out,strlen(str_out));
        memset(str_out, 0, sizeof(str_out));


    }

    /////////////////////////////logging starts here //////////////////////////////////

    int offset_saver;
    int entry_saver;

    if (logging_enabled == true){

    // put the first line of log in buffer
    sprintf(str_out, "%s /%s length %d\n",message->method,message->filename, message->content_length);
    


    pthread_mutex_lock(&offset_lock);
    //increment offset portion should be locked for thread safety

    offset_saver = log_offset;

    log_offset += strlen(str_out);  //first line of the log message
    log_offset +=  ( (message->content_length) / 20 ) * 69 ; // space for n-1 lines of hex dump
    log_offset += 8 + ( (message->content_length)%20 )*3 +1 ;// space for last line of hex dump
    log_offset += 9;  // space for last line of log

    entry_saver = entries_in_log;

    entries_in_log +=1;

    //************************************************//
    pthread_mutex_unlock(&offset_lock);


    // everytime we write to log_file increment str_out
    nw = pwrite(log_fd, str_out, strlen(str_out), offset_saver);
    offset_saver += nw;

    /////////////////logging ends here ///////////////////////////////

    }


    

    int bytes_to_write = message->content_length;
    int bytes_requested;
    int bytes_written;
    int j;

    int log_char;

    int bytes_read;

    char full_str[BUFFER_SIZE+1];
    memset(full_str, 0, sizeof(full_str));


     int i = 0;
     int e = 0;

    while(bytes_to_write>0){


         if ( bytes_to_write > BUFFER_SIZE ){
        bytes_requested = BUFFER_SIZE;
         }
         else{
            bytes_requested = bytes_to_write;
         }

        if( sending_healthcheck == false ){
            bytes_read = read(in_fd, full_str, bytes_requested);
        }
        else{
            sprintf(full_str, "%d\n%d", errors_in_log, entry_saver );
            message->content_length = strlen(str_out);
            bytes_read = strlen(full_str);

        }
    
        
        //offset_saver holds the offset
        //

        ///////////////// logging is here //////////////////////////
        

        if (logging_enabled== true){

        

        for (j=0; j<bytes_requested; j++){
            
            if (i%20 ==0 ){

                if (i> 0 ){
                    memset(str_out, 0, sizeof(str_out));
                    strcpy(str_out,"\n" );
                    nw = pwrite(log_fd, str_out, strlen(str_out), offset_saver);
                    offset_saver += nw;

                }
                memset(str_out, 0, sizeof(str_out));
                sprintf(str_out,"%08d", i );
                nw = pwrite(log_fd, str_out, strlen(str_out), offset_saver);
                offset_saver += nw;

            }
            e = full_str[j];
            memset(str_out, 0, sizeof(str_out));
            if(e<16){
                sprintf(str_out, " %02x", (unsigned char) e);
            }
            else{
                sprintf(str_out, "%3x", (unsigned char) e);
            }
            
            nw = pwrite(log_fd, str_out, strlen(str_out), offset_saver);
            offset_saver += nw;
            i++;
        }


            
        ///////////////// logging ends here //////////////////////////

        }

        //write to the out fd (for put its the openfile for get its the client)
        bytes_written = write(out_fd,full_str,bytes_requested);
        bytes_to_write -= bytes_written;

        }

    
        memset(str_out, 0, sizeof(str_out));
        strcpy(str_out,"\n========\n");
        nw = pwrite(log_fd, str_out, strlen(str_out), offset_saver);
        offset_saver = offset_saver + nw;
}

void construct_http_response(ssize_t client_sockd, struct httpObject* message) {


    char str_out[BUFFER_SIZE +1];
    memset(str_out, 0, sizeof(str_out));

     if(check_filename(message->filename)==false  && s_code_err(message->status_code) == false ){
            strcpy(message->status_code,"400 Bad Request");
            message->error_number = 400;

       }

       // added a flag for invalid http.

     if (valid_methodname(message) == false && s_code_err(message->status_code) == false ){
        strcpy(message->status_code,"400 Bad Request");
            message->error_number = 400;
     }

     

     if (strcmp("healthcheck", message->filename) == 0){
         if ( strcmp("PUT", message->method) == 0 || strcmp("HEAD", message->method) == 0 ){
            strcpy(message->status_code,"403 Forbidden");
            message->error_number = 403;
         }
         else if (logging_enabled == false){
            strcpy(message->status_code,"404 Not Found");
            message->error_number = 404;
         }
         else{
            gen_response(0,client_sockd,message,str_out);
            close(client_sockd);
            return;
         }
     }

     // check for healthcheck
     // if logfile not enabled throw 404
     // if Head or put throw 403
     // everytime i log, save number of errors and total posts




    if (s_code_err(message->status_code)) {

        errcode_respond(client_sockd,message,str_out);

        close(client_sockd);
        return;
    }

    if(strcmp("PUT", message->method) == 0){

        
        //create the file and update the status code
       int errnum = errno;
       int write_fd = open(message->filename, O_TRUNC | O_RDWR | O_CREAT, 0777 );
       strcpy(message->status_code,"201 Created");


      

       
       gen_response(client_sockd,write_fd,message, str_out);


       memset(str_out, 0, sizeof(str_out));
       sprintf(str_out, "%s %s \r\nContent-Length: 0\r\n\r\n",message->httpversion,message->status_code);
       write(client_sockd,str_out,strlen(str_out));

       close(write_fd);
       close(client_sockd);
       return;

      }
        else if(strcmp(message->method,"GET")==0){


        //now we want to check for the file,construct a response
        // and send the response back to the client
        int errnum = errno;
        //try to read from the file

        int read_fd = open(message->filename, O_RDONLY);

        // this assumes that if read_fd is -1 file DNE
        if(read_fd == -1){

            if(errno ==2){
                strcpy(message->status_code,"404 Not Found");
                message->error_number = 404;
            }else if(errno ==13){
                strcpy(message->status_code,"403 Forbidden");
                message->error_number = 403;
            }else{
                strcpy(message->status_code,"500 Internal Server Error");
                message->error_number = 500;
            }

            
            errcode_respond(client_sockd,message,str_out);

            close(read_fd);
            close(client_sockd);
            return;
        }

        gen_response(read_fd,client_sockd,message,str_out);
       
        close(read_fd);
        close(client_sockd);

    
    } else if (strcmp(message->method,"HEAD")==0) {


        int read_fd = open(message->filename, O_RDONLY);

    

        if(read_fd == -1){

            if(errno ==2){
                strcpy(message->status_code,"404 Not Found");
                message->error_number = 404;
            }else if(errno ==13){
                strcpy(message->status_code,"403 Forbidden");
                message->error_number = 403;
            }else{
                strcpy(message->status_code,"500 Internal Server Error");
                message->error_number = 500;
            }

            
            errcode_respond(client_sockd,message,str_out);


            close(read_fd);
            close(client_sockd);
            return;
        }

        struct stat fileStat;
        fstat(read_fd, &fileStat);
        message->content_length = fileStat.st_size;


        strcpy(message->status_code,"200 OK");
        sprintf(str_out, "%s %s\r\nContent-Length: %d\r\n\r\n",message->httpversion,message->status_code,message->content_length);
        write(client_sockd,str_out,sizeof(str_out));

        memset(str_out, 0, sizeof(str_out));

        

        /////////////////logging starts here//////////////////////////////////

        if(logging_enabled == true){

        

        // put the first line of log in buffer
        sprintf(str_out, "%s /%s length %d\n",message->method,message->filename, message->content_length);
        sprintf(str_out + strlen(str_out),"========\n");
        int offset_saver = log_offset;


        pthread_mutex_lock(&offset_lock);
        //increment offset portion should be locked for thread safety
        log_offset += strlen(str_out);  //first line of the log message

     
        entries_in_log +=1;

        //************************************************//
        pthread_mutex_unlock(&offset_lock);


        // everytime we write to log_file increment str_out
        pwrite(log_fd, str_out, strlen(str_out), offset_saver);    


        //////////////////////////logging ends here /////////////////////////////////

        }

        memset(str_out, 0, sizeof(str_out));

        close(client_sockd);
        close(read_fd);
        
    }
    else{
        strcpy(message->status_code,"500 Internal Server Error");
        message->error_number = 500;
        sprintf(str_out, "%s %s \r\nContent-Length: 0\r\n\r\n",message->httpversion,message->status_code);
        write(client_sockd,str_out,sizeof(str_out));
        close(client_sockd);
    } 
    
    memset(str_out, 0, sizeof(str_out));
    return;
}

static void *worker_thread(void *arg)
{

    int worker_fd;
    struct httpObject message;

    while(1) {

        pthread_mutex_lock(&mutex);
        while(queue_size < 1) {
            pthread_cond_wait(&cond, &mutex);
        }

        
        //Extract work from queue
        --queue_size;
        worker_fd = dequeue(queue_ptr);

        pthread_mutex_unlock(&mutex);

        read_http_request(worker_fd, &message);
        construct_http_response(worker_fd, &message);
        
        memset(&message,0,sizeof(message));

    }
    return 0;
}


int main(int argc, char** argv)
{


    int log_flag = 0;
    int bflag = 0;
    char* num_threads = NULL;
    int num_threads_val = 4;
    int log_file_val = 0 ;
    char* log_file = NULL;
    int port_number = 8080;

    int index;
    int c;

    opterr = 0;

    while ((c = getopt(argc, argv, "l:bN:")) != -1) {
        switch (c) {
            case 'l':
                log_flag = 1;
                log_file = optarg;
                //log_file_val = atoi(optarg);

                break;
            case 'b':
                bflag = 1;
                break;
            case 'N':
                num_threads = optarg;
                num_threads_val = atoi(num_threads);
                break;
            
        }
    }

    for (index = optind; index < argc; index++) {
        port_number = atoi(argv[index]);

    }

    if (log_flag == 1){
        logging_enabled = true;
    }
    else{
        logging_enabled = false;
    }

    
    int nthreads = num_threads_val;
    


    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_number);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    socklen_t addrlen = sizeof(server_addr);

    /*
        Create server socket
    */
    int server_sockd = socket(AF_INET, SOCK_STREAM, 0);

    // Need to check if server_sockd < 0, meaning an error
    if (server_sockd < 0) {
        perror("socket");
    }

    /*
        Configure server socket
    */
    int enable = 1;

    /*
        This allows you to avoid: 'Bind: Address Already in Use' error
    */
    int ret = setsockopt(server_sockd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    /*
        Bind server address to socket that is open
    */
    ret = bind(server_sockd, (struct sockaddr *) &server_addr, addrlen);

    struct sockaddr client_addr;
    socklen_t client_addrlen;

    


    ret = listen(server_sockd, SOMAXCONN);

    if (ret < 0) {
		    perror("Listen Error");
		    return (errno);
        }

    pthread_mutex_init(&mutex, 0);
    pthread_cond_init(&cond, 0);

    pthread_t thread[nthreads];

    //create worker threads
    for(int i=0; i<nthreads; i++){
        pthread_create(&(thread[i]), 0, worker_thread, (void*)(i+1));
    }

    queue_size =0;
    queue_ptr = createQueue(10000);

    if (logging_enabled == true){
        log_offset = 0;
        errors_in_log = 0;
        entries_in_log = 0;
        log_fd = open(log_file, O_TRUNC | O_RDWR | O_CREAT, 0777 );
    }

    

    if (pthread_mutex_init(&offset_lock, NULL) != 0) { 
        return 1; 
    } 

    while(true){


        client_fd = accept(server_sockd, &client_addr, &client_addrlen);

        int errnum = errno;
        if(client_fd ==-1){
        }

        pthread_mutex_lock(&mutex);
        //Add work to queue
        queue_size++;
        enqueue(queue_ptr,client_fd);

        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&mutex);
    }

    return 0;
} 
