/* overlap2.c - test overlapping segments */

#include "rvm.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

#define STR1 "AAAAA"
#define STR2 "BBBBB"
#define OFFSET 100


/* proc1 writes some data, commits it, then exits */
void proc1() 
{
     rvm_t rvm;
     trans_t trans;
     char* segs[1];
     
     rvm = rvm_init("rvm_segments");
     rvm_destroy(rvm, "seg1");
	 rvm_destroy(rvm, "seg2");
     segs[0] = (char*) rvm_map(rvm, "seg1", 1000);
     
     trans = rvm_begin_trans(rvm, 1, (void **) segs);
     
     rvm_about_to_modify(trans, segs[0], OFFSET + 0, 6);
     sprintf(segs[0] + OFFSET + 0, STR1);
     
     rvm_about_to_modify(trans, segs[0], OFFSET + 3, 6);
     sprintf(segs[0] + OFFSET + 3, STR2);
     
     rvm_commit_trans(trans);

	 trans = rvm_begin_trans(rvm, 1, (void **) segs);

	 rvm_about_to_modify(trans, segs[0], OFFSET + 2, 4);
	 sprintf(segs[0] + OFFSET + 2, "CCC");

     abort();
}


/* proc2 opens the segments and reads from them */
void proc2() 
{
     char* segs[1];
     rvm_t rvm;
     
     rvm = rvm_init("rvm_segments");

     segs[0] = (char *) rvm_map(rvm, "seg1", 1000);

     if(strcmp(segs[0]+OFFSET, "AAABBBBB")) {
	  printf("ERROR: string not correct: %s\n", segs[0]+OFFSET);
	  exit(2);
     }

     printf("OK\n");
     exit(0);
}


int main(int argc, char **argv)
{
     int pid;

     pid = fork();
     if(pid < 0) {
	  perror("fork");
	  exit(2);
     }
     if(pid == 0) {
	  proc1();
	  exit(0);
     }

     waitpid(pid, NULL, 0);

     proc2();

     return 0;
}
