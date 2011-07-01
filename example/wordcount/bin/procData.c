#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
int MAXNUM = 50000000;
int main(int argc, char *argv[]){
	FILE *fp=NULL;
	assert((fp=fopen("./largdata.txt","w")) != NULL);
	int i=0,tmp,j=0,k=0;
	char ch[5] = {NULL};
	srand((int)time(0));
	if (argc > 1) 
		MAXNUM = atoi(argv[1]);
	while(i<MAXNUM){
		tmp = rand();
		for(k=0;k<5;k++){
			for(j=0;j<3;j++){
				ch[j] = 'a' + rand()%26;
			}
			ch[3]=' ';
			ch[4] = '\0';
			assert(fprintf(fp,"%s",ch));
		}
		assert(fprintf(fp,"\n"));
		i++;
	}
	fclose(fp);
}
