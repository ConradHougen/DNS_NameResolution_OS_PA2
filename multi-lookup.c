/*
 * File: multi-lookup.c
 * Author: Conrad Hougen
 * Project: CSCI 3753 Programming Assignment 2
 * Create Date: 
 * Modify Date: 
 * Description:
 * 	This file provides a multi-threaded solution
 * 		to the DNS lookup assignment (PA2).
 *  
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include "queue.h"
#include "util.h"
#include <pthread.h>


#define USAGE "<inputFilePath> <outputFilePath>"
#define INPUTFS "%1024s"

#define MIN_INPUT_FILES 1
#define MAX_INPUT_FILES 10
#define MAX_RESOLVER_THREADS 10
#define MIN_RESOLVER_THREADS 2
#define MAX_NAME_LENGTH 1025
#define MAX_REQUESTER_THREADS 10
#define MIN_REQUESTER_THREADS 2

#define MINARGS (MIN_INPUT_FILES+2)
#define MAXARGS (MAX_INPUT_FILES+2)
#define SBUFSIZE MAX_NAME_LENGTH

// option for running with multi-IP resolution (up to MULTI_IP IP addresses)
#define MULTI_IP 40

// shared request queue
queue request_q;
pthread_mutex_t q_mutex = PTHREAD_MUTEX_INITIALIZER;
const int QSIZE = 10;

// mutex for the output file
pthread_mutex_t fileout_mutex = PTHREAD_MUTEX_INITIALIZER;

// global var to keep track of number of active requester threads
unsigned int num_req_thrds = MAX_REQUESTER_THREADS;
pthread_mutex_t count_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Function for requester threads to run */
void* req_thrd_func(void* fileIn)
{
	char *fileStr = (char *)fileIn;
	FILE *inputfp = NULL;
	
	printf("This is the file that I will process: %s\n", fileStr);
	
	// open the input file
	inputfp = fopen(fileStr, "r");
	if(!inputfp)
	{
	    fprintf(stderr, "Error Opening Input File: %s", fileStr);
	    pthread_mutex_lock(&count_mutex);
	    num_req_thrds--;
	    pthread_mutex_unlock(&count_mutex);
	    return NULL;
	}
	
	while(1)
	{
		char *hostname = (char *)malloc(sizeof(char) * SBUFSIZE);
		// check for failed malloc
		if(hostname == NULL)
		{
			fprintf(stderr, "heap full; req thrd hostname malloc failed\n");
			break;
		}
		// No matches, so we are done --> break out of loop
		if(fscanf(inputfp, INPUTFS, hostname) <= 0)
		{
			free(hostname);
			break;
		}
		
		pthread_mutex_lock(&q_mutex);
		// critical section
		while(queue_is_full(&request_q))
		{
			// give up mutex, sleep or a random amount of time in [0,100] microseconds
			pthread_mutex_unlock(&q_mutex);
			usleep((rand()%101));
			// reacquire mutex
			pthread_mutex_lock(&q_mutex);
		}
		printf("Pushing to q: %s\n", hostname);
		queue_push(&request_q, hostname);
		pthread_mutex_unlock(&q_mutex);
	}
	
	// Close Input File 
	fclose(inputfp);
	printf("Requester thread exiting...\n");
	
	pthread_mutex_lock(&count_mutex);
	num_req_thrds--;
	pthread_mutex_unlock(&count_mutex);
	
	return NULL;
}

/* Function for resolver threads */
void* res_thrd_func(void* fileOut)
{
	char *fileStr = (char *)fileOut;
	FILE *outputfp = NULL;
#ifdef MULTI_IP
	queue IP_q;
	queue_init(&IP_q, MULTI_IP);
	char *tempstr = NULL;
	int i;
#else
	char firstipstr[INET6_ADDRSTRLEN];
#endif
	char *hostname = NULL;
	
	printf("This is the file that I will write: %s\n", fileStr);
	pthread_mutex_lock(&count_mutex);
	while(num_req_thrds > 0)
	{
		pthread_mutex_unlock(&count_mutex);
		pthread_mutex_lock(&q_mutex);
		if(!queue_is_empty(&request_q))
		{
			// pop a hostname off the queue
		    hostname = (char *)queue_pop(&request_q);
			printf("Popping from queue %s\n", hostname);
			pthread_mutex_unlock(&q_mutex);

			// open the output file
			pthread_mutex_lock(&fileout_mutex);
			outputfp = fopen(fileStr, "a");
			if(!outputfp)
			{
				fprintf(stderr, "Error Opening Output File %s\n", fileStr);
				pthread_mutex_unlock(&fileout_mutex);
				return NULL;
			}

#ifdef MULTI_IP
			// do the dns lookup with multiple IP addresses
			if(dnslookup_multiIP(hostname, &IP_q) == UTIL_FAILURE)
			{
				fprintf(stderr, "dnslookup error: %s\n", hostname);
				char *errstr = "";
				// write the output file
				fprintf(outputfp, "%s,%s\n", hostname, errstr);
			}
			else
			{
				// write the output file with all ip addresses
				fprintf(outputfp, "%s", hostname);
				for(i = 0; i < MULTI_IP; ++i)
				{
					if(!queue_is_empty(&IP_q))
					{
						tempstr = (char *)queue_pop(&IP_q);
						fprintf(outputfp, ",%s", tempstr);
						if(tempstr != NULL)
						{
							free(tempstr);
						}
					}
					else
					{
						if(i == 0)
						{
							fprintf(outputfp, ",");
						}
						break;
					}
				}
				// finish print for this hostname
				fprintf(outputfp, "\n");
			}
			if(!queue_is_empty(&IP_q))
			{
				// something weird happened
				fprintf(stderr, "Unknown ip string queue error occurred\n");
				while(!queue_is_empty(&IP_q))
				{
					tempstr = (char *)queue_pop(&IP_q);
					if(tempstr != NULL)
					{
						free(tempstr);
					}
				}
			}	
#else
			// do the dns lookup
			if(dnslookup(hostname, firstipstr, sizeof(firstipstr)) == UTIL_FAILURE)
			{
				fprintf(stderr, "dnslookup error: %s\n", hostname);
				strncpy(firstipstr, "", sizeof(firstipstr));
			}
			// write the output file
			fprintf(outputfp, "%s,%s\n", hostname, firstipstr);
#endif
		   
			// close the output file
			fclose(outputfp);
			pthread_mutex_unlock(&fileout_mutex);
	    
			// free the heap memory for the hostname
			free(hostname);
		}
		else 
		{
			pthread_mutex_unlock(&q_mutex);
		}
	    pthread_mutex_lock(&count_mutex);
	}
	pthread_mutex_unlock(&count_mutex);
	
	// cleanup lingering requests
	pthread_mutex_lock(&q_mutex);
	while(!queue_is_empty(&request_q))
	{
	  // pop a hostname off the queue
	  hostname = (char *)queue_pop(&request_q);
	  printf("Popping from queue %s\n", hostname);
	  pthread_mutex_unlock(&q_mutex);
	  
	  // open the output file
	  pthread_mutex_lock(&fileout_mutex);
	  outputfp = fopen(fileStr, "a");
	  if(!outputfp)
	  {
	      printf("Error Opening Output File\n");
	      pthread_mutex_unlock(&fileout_mutex);
	      return NULL;
	  }

#ifdef MULTI_IP
			// do the dns lookup with multiple IP addresses
			if(dnslookup_multiIP(hostname, &IP_q) == UTIL_FAILURE)
			{
				fprintf(stderr, "dnslookup error: %s\n", hostname);
				char *errstr = "";
				// write the output file
				fprintf(outputfp, "%s,%s\n", hostname, errstr);
			}
			else
			{
				// write the output file with all ip addresses
				fprintf(outputfp, "%s", hostname);
				for(i = 0; i < MULTI_IP; ++i)
				{
					if(!queue_is_empty(&IP_q))
					{
						tempstr = (char *)queue_pop(&IP_q);
						fprintf(outputfp, ",%s", tempstr);
						if(tempstr != NULL)
						{
							free(tempstr);
						}
					}
					else
					{
						if(i == 0)
						{
							fprintf(outputfp, ",");
						}
						break;
					}
				}
				// cleanup
				fprintf(outputfp, "\n");
			}
			if(!queue_is_empty(&IP_q))
			{
				// something weird happened
				fprintf(stderr, "Unknown ip string queue error occurred\n");
				while(!queue_is_empty(&IP_q))
				{
					tempstr = (char *)queue_pop(&IP_q);
					if(tempstr != NULL)
					{
						free(tempstr);
					}
				}
			}		
#else
	  // do the dns lookup
	  if(dnslookup(hostname, firstipstr, sizeof(firstipstr)) == UTIL_FAILURE)
	  {
	      fprintf(stderr, "dnslookup error: %s\n", hostname);
	      strncpy(firstipstr, "", sizeof(firstipstr));
	  }
	  // write the output file
	  fprintf(outputfp, "%s,%s\n", hostname, firstipstr);
#endif
	  // close the output file
	  fclose(outputfp);
	  pthread_mutex_unlock(&fileout_mutex);

	  // free the heap memory for the hostname
	  free(hostname);

	  pthread_mutex_lock(&q_mutex);
	}
	pthread_mutex_unlock(&q_mutex);

#ifdef MULTI_IP
	// cleanup the ip address string queue (not shared between threads)
	queue_cleanup(&IP_q);
#endif
	return NULL;
}

int main(int argc, char* argv[]){

    /* Create Requester and Resolver thread pools */
    pthread_t requester[MAX_REQUESTER_THREADS];
    pthread_t resolver[MAX_RESOLVER_THREADS];
    int rc_res, rc_req;
    int t, num_cores;
    int tid_res[MAX_RESOLVER_THREADS], tid_req[MAX_REQUESTER_THREADS];
    void *exit_status = NULL;
    FILE *test_outfile = NULL;
    
    /* Check Arguments */
    if(argc < MINARGS || argc > MAXARGS){
		fprintf(stderr, "Improper number of args: %d\n", (argc - 1));
		fprintf(stderr, "Usage:\n %s %s\n", argv[0], USAGE);
		return EXIT_FAILURE;
    }

    // check for bogus output file path
    if(!(test_outfile = fopen(argv[argc-1], "w")))
    {
		fprintf(stderr, "Bogus output file path...exiting\n");
		fprintf(stderr, "Usage:\n %s %s\n", argv[0], USAGE);
		return -1;
    }
    fclose(test_outfile);

    // get the number of processor cores on the system and create that many resolvers
    num_cores = get_nprocs_conf();
    printf("Number of processor cores on system: %d\n", num_cores);

    // initialize request queue
    if(queue_init(&request_q, QSIZE) == QUEUE_FAILURE){
		fprintf(stderr, "error: queue_init failed!\n");
		exit(EXIT_FAILURE);
    }
    
	/* create threads in requester thread pool */
	num_req_thrds = argc-2;
	for(t=0; t < (argc-2); ++t)
	{
		// start the tid count after the last tid for the resolvers
		tid_req[t] = num_cores+t;
		printf("Creating requester thread %d\n", tid_req[t]);
		rc_req = pthread_create(&(requester[t]), NULL, req_thrd_func, argv[t+1]);
		if(rc_req){
			printf("ERROR; return code from pthread_create() is %d\n", rc_req);
			exit(EXIT_FAILURE);
		}
	}
	
	/* create threads in resolver thread pool */
	for(t=0; t < num_cores; ++t)
	{
		tid_res[t] = t;
		printf("Creating resolver thread %d\n", tid_res[t]);
		rc_res = pthread_create(&(resolver[t]), NULL, res_thrd_func, argv[argc-1]);
		if (rc_res){
			printf("ERROR; return code from pthread_create() is %d\n", rc_res);
			exit(EXIT_FAILURE);
		}		
	}
	
	/* join the requester threads */
	for(t=0; t < (argc-2); ++t){
		rc_req = pthread_join(requester[t], exit_status);
		if(rc_req)
		{
		  printf("ERROR; return code from pthread_join() is %d\n", rc_req);
		  exit(EXIT_FAILURE);
		}
	}
	printf("All of the requester threads completed!\n");

	/* join the resolver threads */
	for(t=0; t < num_cores; ++t){
		rc_res = pthread_join(resolver[t], exit_status);
		if(rc_res)
		{
		    printf("ERROR; return code from pthread_join() is %d\n", rc_res);
		    exit(EXIT_FAILURE);
		}
	}
	printf("All of the resolver threads completed!\n");

	
	// cleanup the queue
	pthread_mutex_lock(&q_mutex);
	queue_cleanup(&request_q);
	pthread_mutex_unlock(&q_mutex);
	
	// destroy mutexes
	pthread_mutex_destroy(&q_mutex);
	pthread_mutex_destroy(&fileout_mutex);
	pthread_mutex_destroy(&count_mutex);
	

    return EXIT_SUCCESS;
}
