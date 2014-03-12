#include <stdio.h>
#include <unistd.h>
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#define JQSIZE  10
#define LSIZE	6

#define SENDER	10
#define RECEIVER 11
#define OK 12

#define SENDMSG 1
#define RECVMSG 0

#define REQTAG 5
#define WORKTAG 6

typedef struct _range{
        int start;
        int end;
}range;

int running = 1;
int dyn_qsize;
int fact_num;
int job_next = 0;
int qsize;

range jobque[JQSIZE];

int ok_list[LSIZE];			//lists
int sender_list[LSIZE];
int recvr_list[LSIZE];

					//list indexers
int send_index = 1;	//initially empty
int recv_index = 1;	//start from first recv_worker list
int ok_index = 1;	//initially empty

int tresh_snd = 0;			//tresholds
int tresh_rcv = 0;

		//number of receivers, senders, oks
int num_receivers;
int num_senders = 0;
int num_oks = 0;


using namespace std;

// trim
void trim(string &str) 
{
	for (int i=0;i<str.length();i++)
	if (str[i] == ' ') 
	{
		str.erase(i,1);
		i--;
	}
}
//

//trial division
void trial_div(int start, int end, int fact_num, int myid)
{

	cout<<myid<<" :: calculating range : "<<start<<" - "<<end<<endl;
	while(start<=end)
        {
        	if(fact_num % start == 0)
		{
                	cout<<myid<<" :: Found prime : "<<start<<endl;
		}
		start++;
        }

}
// 

//LOCKS 
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex3 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex4 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex5 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex6 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex7 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex8 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex9 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex10 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex11 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex12 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex13 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex14 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex15 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex16 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex17 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex18 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex19 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex20 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex21 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex22 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex23 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex24 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex25 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex26 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex27 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex28 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex29 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex30 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex31 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex32 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex33 = PTHREAD_MUTEX_INITIALIZER;

//


//request comm

int sendreq;
int recvreq;
int okreq;
int counter = 0;
int remstart, remend;   //range values to work
//
//mpi listener
void *listen(void *dm)
{
	int id = (long)dm;

	while(running)
	{
		//cout<<"running ... "<<endl;
		MPI_Status rstatus;
		int coming = -1;
		int response;
		int sid;

		//listen for msg
		MPI_Recv(&coming, 2, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &rstatus);
		sid = rstatus.MPI_SOURCE;
		//cout<<id<<" :: message received - "<<id<<endl;

		if(coming == SENDMSG)			//sender conn
		{
			if(dyn_qsize < tresh_rcv)	//recv response
				response = RECEIVER;
			else if(dyn_qsize > tresh_snd)	//recv response
				response = SENDER;
			else
				response = OK;

//attent!!!		//remove requester from the recvrs list
			sid = rstatus.MPI_SOURCE;
pthread_mutex_lock( &mutex1 );
			recvr_list[sid] = 0;

			//add to the senders list
			sender_list[send_index] = sid;
			send_index++;
			num_senders++;

pthread_mutex_unlock( &mutex1 );

			MPI_Send(&response, 1, MPI_INT, sid, REQTAG, MPI_COMM_WORLD);

			if(response == RECEIVER)
			{
				int rstart, rend;	//range values to work
							//recv start
				MPI_Recv(&rstart, 1, MPI_INT, sid, WORKTAG, MPI_COMM_WORLD, &rstatus);
							//recv end
				MPI_Recv(&rend, 1, MPI_INT, sid, WORKTAG, MPI_COMM_WORLD, &rstatus);

				//if(job_next<qsize){
				cout<<id<<" :: ******* received range "<<rstart<<" - "<<rend<<" from "<<sid<<endl;
pthread_mutex_lock( &mutex2 );
					dyn_qsize++;
					trial_div(rstart, rend, fact_num, id);
					dyn_qsize--;
pthread_mutex_unlock( &mutex2 );
				//}

			}

		}
		else if(coming == RECVMSG)		//receiver conn
		{

			if(dyn_qsize < tresh_rcv)       //recv response
                                response = RECEIVER;
                        else if(dyn_qsize > tresh_snd)  //send response
                                response = SENDER;
                        else				//ok response
                                response = OK;

			sid = rstatus.MPI_SOURCE;
pthread_mutex_lock( &mutex3 );
			recvr_list[sid] = sid;
pthread_mutex_unlock( &mutex3 );
			MPI_Send(&response, 1, MPI_INT, sid, REQTAG, MPI_COMM_WORLD);

			if(response == SENDER)
                        {
				//send range 
				if(job_next<qsize)
				{
					int rstart = jobque[job_next].start;
                                        int rend = jobque[job_next].end;

                                        //send range start
                                        MPI_Send(&rstart, 1, MPI_INT, sid, WORKTAG, MPI_COMM_WORLD);

                                        //send range end
                                        MPI_Send(&rend, 1, MPI_INT, sid, WORKTAG, MPI_COMM_WORLD);
pthread_mutex_lock( &mutex4 );
					job_next++;
                                        dyn_qsize--;
pthread_mutex_unlock( &mutex4 );
                                }

			}
//////here 
		}

		else
		{

///////////////////////////////
pthread_mutex_lock( &mutex26 );

			if(dyn_qsize < tresh_rcv)       //recv response
                                response = RECEIVER;
                        else if(dyn_qsize > tresh_snd)  //recv response
                                response = SENDER;
                        else
                                response = OK;
pthread_mutex_unlock( &mutex26 );

		//responses
			if(coming == RECEIVER){
pthread_mutex_lock( &mutex20 );
				sendreq = 1;
pthread_mutex_unlock( &mutex20 );
			}

			if(coming == SENDER){
pthread_mutex_lock( &mutex28 );
				recvreq = 1;
pthread_mutex_unlock( &mutex28 );

			}

                        if(coming == OK){
pthread_mutex_lock( &mutex27 );
                                okreq = 1;
pthread_mutex_unlock( &mutex27 );
			}

			else if(coming!=OK && coming!=RECEIVER && coming!=SENDER){
				//get numbers
pthread_mutex_lock( &mutex24 );
				counter++;
				if(counter == 1){
 					remstart = coming;
                                        cout<<id<<" :: first number -- "<<remstart<<" from node - " <<sid<<endl;

				}
				if(counter == 2){

					remend = coming;
                                        cout<<id<<" :: second number -- "<<remend<<" from node - " <<sid<<endl;
					counter = 0;
		//new
					  trial_div(remstart, remend, fact_num, id);
                                }

pthread_mutex_unlock( &mutex24 );

			}
			
			//do nothing
			//cout<<id<<"_____INVALID MSG______ : "<<coming<<endl;
		}
		//cout<<id<<" :: -------------------  listen() - msg received : "<<coming<<endl;
	}
	return 0;
}

//

///


int main(int argc, char *argv[]) 
{

//mpi trial_division properties-start
//	int tresh_snd = 0;
//	int tresh_rcv = 0;
	int poll_limit = 0;
	int num_of_procs = 0;
	int *num_of_ranges = NULL;
	string machines_file;
	string ranges_file;
	range *ranges = NULL;
	int total_nranges = 0;

//mpi trial_division properties-end

	int myid, np;
	char hostname[128];

	MPI_Init(&argc,&argv);								// starts MPI
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);	// get index of process
	MPI_Comm_size(MPI_COMM_WORLD, &np); 	// get number of processes

	gethostname(hostname,128);

	string line;
					//params
	string conf_file(argv[1]);		//configuration file
	int num1 = atoi(argv[2]);		//numbers to factor the product
	int num2 = atoi(argv[3]);

	//int fact_num = num1 * num2;		//number to factor
	fact_num = num1 * num2;
	cout<<"Number to factor: "<<fact_num<<endl;


//	cout<<"conf file name: "<<conf_file<<endl;

	ifstream confile(argv[1]);
	if(confile.is_open())
	{
		while(confile.good())
		{
			getline(confile, line);
			trim(line);
//			cout<<"### line read : "<<line<<endl;

			int i = line.find("=");
			string property = line.substr(0,i);
//			cout<<"property :"<<property;

			string prop_val = line.substr(i+1);
//			cout<<" :: property value : "<<prop_val<<endl;

			if(property.compare("t_sender")==0)
			{
				tresh_snd = atoi(prop_val.c_str());
//				cout<<tresh_snd<<endl;
			}else if(property.compare("t_recv")==0)
			{
				tresh_rcv = atoi(prop_val.c_str());
//                                cout<<tresh_rcv<<endl;
			}else if(property.compare("poll_limit")==0)
                        {
                                poll_limit = atoi(prop_val.c_str());
//                                cout<<poll_limit<<endl;
                        }else if(property.compare("num_of_procs")==0)
                        {
                                num_of_procs = atoi(prop_val.c_str());
//                                cout<<num_of_procs<<endl;
				num_of_ranges = new int[num_of_procs];
				//ranges_each = new *range[num_of_procs];

                        }else if(property.compare("num_of_ranges")==0)
                        {
				num_of_ranges[0] = 0;
				for(int k=1; k<num_of_procs;k++){
					size_t j = prop_val.find(",");
                        		if(j == string::npos)
					{
						num_of_ranges[k] = atoi(prop_val.c_str());
					}else{
						string val = prop_val.substr(0,j);
						num_of_ranges[k] = atoi(val.c_str());
						prop_val = prop_val.substr(j+1);
					}
				}

				//get total number of ranges:
			//	int total_nranges = 0;

				//cout<<"Ranges for all : "<<endl;
				for(i=0;i<num_of_procs;i++)
					total_nranges += num_of_ranges[i];
				//cout<<"Total ranges for all : "<<total_nranges<<endl;
				ranges = new range[total_nranges+1];

			}else if(property.compare("machines_file")==0)
			{
				machines_file = prop_val;
//				cout<<machines_file<<endl;

			}else if(property.compare("ranges_file")==0)
                        {
                                ranges_file = prop_val;
//                                cout<<ranges_file<<endl;
                        }
		}

		confile.close();


		//read all ranges - start
		ifstream rfile(ranges_file.c_str());
		if(rfile.is_open()){
			ranges[0].start = 0; ranges[0].end = 0;			//init ranges[0]
			int ii = 1;
			while(rfile.good())
			{
				if(ii<=total_nranges)
				{
					string liner;
					getline(rfile, liner);
//					cout<<liner<<endl;
					int ji = liner.find("-");
//					cout<<atoi(liner.substr(0,ji).c_str())<<endl;
//					cout<<atoi(liner.substr(ji+1).c_str())<<endl;
					//ranges[ii].start = atoi(liner.substr(0,ji).c_str());
					ranges[ii].start = atoi(liner.substr(0,ji).c_str());
					ranges[ii].end =  atoi(liner.substr(ji+1).c_str());
//					cout<<" :: "<<ranges[ii].start<<" :: "<<ranges[ii].end<<endl;
					ii++;
				}else
					break;
			}

			rfile.close();
//			cout<<"Ranges: "<<endl;
//			for(ii=0;ii<total_nranges;ii++)
//			{
//				cout<<ii<<" :: "<<ranges[ii].start<<" :: "<<ranges[ii].end<<endl;
//			}

		}
		else
		{
			cout<<"Error openning ranges file...aborting"<<endl;
			if(ranges != NULL)
                		delete [] ranges;
        		if(num_of_ranges !=NULL)
                		delete [] num_of_ranges;
			return -2;
		}
		//read all ranges - end
	}
	else
	{
		cout<<"Error opening configuration file...aborting"<<endl;
		return -1;
	}


////////////////////

      	if (myid == 0) 
      	{
		cout<<myid<<" :: ############################################## MASTER"<<endl;
		//printf("ID: %d , HOSTNAME: %s\n", myid, hostname);


		//cout<<myid<<" :: ##############################################"<<endl<<endl;
	}


	else 
	{
		cout<<myid<<" :: ############################################## WORKER"<<endl;
	//	printf("ID: %d , HOSTNAME: %s\n", myid, hostname);

		int ss = 0;
		//range jobque[JQSIZE];
		//int 
		qsize = num_of_ranges[myid];

		cout<<myid<<" :: qsize :: "<< num_of_ranges[myid]<<endl;

		int val = 1;
		for(int k = 0; k<myid; k++)
		{
			val += num_of_ranges[k];
		}

//create job queue

		for(int f = val; f< val+num_of_ranges[myid]; f++)
		{
			//cout<<myid<<" range :: "<<ranges[f].start<<" - "<<ranges[f].end<<endl;
			jobque[ss] = ranges[f];
			//jobque[ss].start = ranges[f].start;
			//jobque[ss].end = ranges[f].end;
			ss++;
		}

					//print the job queue
//		for(int sl = 0; sl<num_of_ranges[myid]; sl++)
//			cout<<sl<<":"<<jobque[sl].start<<"-"<<jobque[sl].end<<endl; 


//SCHEDULING - starts
/*****************************************************************/
	//initialize recv_list;
//lock just incase
pthread_mutex_lock( &mutex29 );
		 for(int p = 0; p<LSIZE ; p++)
                {
                                recvr_list[p] = 0;
                }
	
		for(int r = 0; r<np ; r++)
		{
			if((r != myid) & (r != 0) )	//exclude master and itself	
				recvr_list[r] = r;
		}

	//init sender and ok lists
		for(int k = 0; k<LSIZE ; k++)
                {
                                sender_list[k] = 0;
                }

		 for(int p1 = 0; p1<LSIZE ; p1++)
                {
                                ok_list[p1] = 0;
                }

pthread_mutex_unlock( &mutex29 );

	//thread listens for connections
		pthread_t pid;
		pthread_create(&pid, NULL, listen, (void *)myid);
	//scheduling variables
//		int job_next = 0;
		int npoll = 0;
		int poll_node;
//		MPI_Status status;

pthread_mutex_lock( &mutex5 );
		dyn_qsize = qsize;
		num_receivers = np-2; //itself and master excluded
pthread_mutex_unlock( &mutex5 );
/****************************************************************/

	//scheduling
		//for(int ik = 0; ik<qsize; ik++)
		while(running)
		{
			if(npoll < poll_limit)				//npoll if
			{

				if(dyn_qsize >= tresh_snd)			//dyn_qsize >t_send
				{
					if(num_receivers > 0)		//recver list not empty
					{

						poll_node = recvr_list[recv_index];
						while(poll_node == 0 && recv_index<LSIZE)
						{
							poll_node = recvr_list[recv_index];
pthread_mutex_lock( &mutex6 );

							recv_index++;
pthread_mutex_unlock( &mutex6 );

						}
if(poll_node!=0 && poll_node<np){
						 cout<<myid<<" :: sender polling receiver to send work : node-"<<poll_node<<endl;
pthread_mutex_lock( &mutex21 );
						sendreq = -1;
						recvreq = -1;
						okreq = -1;
pthread_mutex_unlock( &mutex21 );
						int smsg = SENDMSG;
						MPI_Send(&smsg, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD);
						int resp;
						//MPI_Recv(&resp, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD, &status);
						sleep(2);
						//if(resp == RECEIVER)		//receiver polled
						if(sendreq == 1)
						{
							if(job_next<qsize){
								int rstart = jobque[job_next].start;
								int rend = jobque[job_next].end;

							cout<<myid<<" :: Moving range "<<rstart<<" - "<<rend<<" to "<<poll_node<<endl;
							//send range start
							MPI_Send(&rstart, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD);

							//send range end
							MPI_Send(&rend, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD);
pthread_mutex_lock( &mutex7 );

							job_next++;
							dyn_qsize--;
pthread_mutex_unlock( &mutex7 );

							}
						}
						else				//not receiver polled
						{
pthread_mutex_lock( &mutex8 );
							//cout<<myid<<" :: SENREQ - "<<sendreq<<endl;
							recvr_list[poll_node] = 0; //remove node from recv list
							num_receivers--;

							//if(resp == SENDER)	//add to sender list
							if(recvreq == 1)
							{
								sender_list[poll_node] = poll_node;
								send_index++;
								num_senders++;

							}
							else if(okreq == 1)			//add to ok list
							{
								ok_list[poll_node] = poll_node;
								ok_index++;
								num_oks++;
							}
							recv_index++;	//move next receiver
pthread_mutex_unlock( &mutex8 );

						}
}

						npoll++;
					}
					else				//receiver list empty
					{
						//cout<<myid<<" :: poll limit exceeded!!!"<<endl;
pthread_mutex_lock( &mutex9 );

						//do not poll do the job yourself
						if(job_next<qsize){
							trial_div(jobque[job_next].start, jobque[job_next].end, fact_num, myid);
                        				job_next++;
                        				dyn_qsize--;
						}
pthread_mutex_unlock( &mutex9 );

					}

				}//receiver
				else if(dyn_qsize < tresh_rcv)		//dyn_qsize < t_recv
				{

					if(num_senders > 0)		//sender list not empty	
					{
						poll_node = sender_list[send_index];
						while(poll_node == 0 && send_index <LSIZE)
						{
							poll_node = sender_list[send_index];
pthread_mutex_lock( &mutex10 );
            						send_index++;
pthread_mutex_unlock( &mutex10 );

						}
if(poll_node!=0 && poll_node<np)
{
						cout<<myid<<" :: receiver polling sender to get work : node-"<<poll_node<<endl;
						int smsg = RECVMSG;
pthread_mutex_lock( &mutex22 );
						recvreq = -1;
						okreq = -1;
						sendreq = -1;
pthread_mutex_lock( &mutex22 );
						MPI_Send(&smsg, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD);
						sleep(2);
						//int resp;
                                                //MPI_Recv(&resp, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD, &status);

						//if(resp == SENDER)            //sender polled
						if(recvreq == 1)
                                                {
							//recv range
////heree
							cout<<myid<<" :: Moving range from sender "<<poll_node<<endl;
							//int remstart, remend;	//range values to work
							//recv start
							//MPI_Recv(&rstart, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD, &status);
							//recv end
							//MPI_Recv(&rend, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD, &status);
							sleep(2);
											//perform range
pthread_mutex_lock( &mutex11 );

							dyn_qsize++;
					//hereee		trial_div(remstart, remend, fact_num, myid);
							dyn_qsize--;
pthread_mutex_unlock( &mutex11 );

						}
						else
						{
pthread_mutex_lock( &mutex12 );
							sender_list[poll_node] = 0;	//remove from senders list
							num_senders--;
							send_index++;

							//if(resp == RECEIVER){
							if(sendreq == 1){//	recvr
								recvr_list[poll_node] = poll_node;
								num_receivers++;
							}else if(okreq == 1) {//OK
								ok_list[poll_node] = poll_node;
								num_oks++;
							}
pthread_mutex_unlock( &mutex12 );

						}
}
					}


					else if(num_oks>0)		//ok list not empty
					{
/////
						poll_node = ok_list[ok_index];
						while(poll_node == 0 && ok_index <LSIZE)
						{
							poll_node = ok_list[ok_index];
pthread_mutex_lock( &mutex13 );
            						ok_index++;
pthread_mutex_unlock( &mutex13 );

						}
if(poll_node!=0 and poll_node<np)
{
///////////
pthread_mutex_lock( &mutex25 );
						okreq = -1;
						recvreq = -1;
						sendreq = -1;
pthread_mutex_unlock( &mutex25 );

						cout<<myid<<" :: receiver polling ok to get work : node-"<<poll_node<<endl;
                                                int smsg = RECVMSG;
                                                MPI_Send(&smsg, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD);
						sleep(2);
                                                //int resp;
                                                //MPI_Recv(&resp, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD, &status);
						
						//if(resp == SENDER)            //sender polled
						if(recvreq == 1)
                                                {
							cout<<myid<<" :: Moving range from sender "<<poll_node<<endl;
							sleep(2);
                                                        //recv range
							//int rstart, rend;       //range values to work
                                                        //recv start
                                                        //MPI_Recv(&rstart, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD, &status);
                                                        //recv end
                                                        //MPI_Recv(&rend, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD, &status);
pthread_mutex_lock( &mutex14 );

                                                        //perform range
                                                        dyn_qsize++;
                                             //here          // trial_div(remstart, remend, fact_num, myid);
                                                        dyn_qsize--;

							//remove from ok list to senders list
							sender_list[poll_node] = poll_node;
							ok_list[poll_node] = 0;
							num_oks--;
							num_senders++;
							ok_index++;//update ok index;
pthread_mutex_unlock( &mutex14 );

                                                }
						
						else
                                                {
pthread_mutex_lock( &mutex15 );

                                                      //  if(resp == RECEIVER){
							 if(sendreq == 1){//     recvr
								//remove from ok list to receivers list
                                                       		recvr_list[poll_node] = poll_node;
                                                        	ok_list[poll_node] = 0;
                                                        	num_oks--;
								ok_index++;
                                                                num_receivers++;
                                                        }else if(okreq == 1){//OK
								ok_index++;
                                                        }
pthread_mutex_unlock( &mutex15 );
                                                }
}

///////////////////
					}
					else if(num_receivers>0)	//recvers list not empty
					{

						poll_node = recvr_list[recv_index];
						while((poll_node==0) && (recv_index<LSIZE))
						{
							poll_node = recvr_list[recv_index];
pthread_mutex_lock( &mutex16 );
            						recv_index++;
pthread_mutex_unlock( &mutex16 );

            						//cout<<"polling receiver : node-"<<poll_node<<endl;
						}

/////////////						// cout<<"polling receiver : node-"<<poll_node<<endl;
if(poll_node!=0 && poll_node<np)
{

pthread_mutex_lock( &mutex30 );
                                                okreq = -1;
                                                recvreq = -1;
                                                sendreq = -1;
pthread_mutex_unlock( &mutex30 );

						cout<<myid<<" :: receiver polling receiver to get work : node-"<<poll_node<<endl;
   						int smsg = RECVMSG;
                                                MPI_Send(&smsg, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD);
                                                //int resp;
                                              //  MPI_Recv(&resp, 1, MPI_INT, poll_node, REQTAG, MPI_COMM_WORLD, &status);
						sleep(2);
                                                //if(resp == SENDER)            //sender polled
//hereee
						if(recvreq == 1)
                                                {
							cout<<myid<<" :: Moving range from sender "<<poll_node<<endl;
							 //recv range
                                                        //int rstart, rend;       //range values to work
                                                        //recv start
                                                        //MPI_Recv(&rstart, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD, &status);
                                                        //recv end
                                                        //MPI_Recv(&rend, 1, MPI_INT, poll_node, WORKTAG, MPI_COMM_WORLD, &status);
							sleep(2);
pthread_mutex_lock( &mutex17 );

                                                        //perform range
                                                        dyn_qsize++;
                                                        //trial_div(remstart, remend, fact_num, myid);
                                                        dyn_qsize--;

                                                        //remove from recv list to senders list
                                                        sender_list[poll_node] = poll_node;
                                                        recvr_list[poll_node] = 0;
                                                        num_receivers--;
                                                        num_senders++;
                                                        recv_index++;//update ok index;
pthread_mutex_unlock( &mutex17 );

						}
						else
                                                {
pthread_mutex_lock( &mutex31 );
							//cout<<"hereeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"<<endl;
                                                       // if(resp == OK){
							if(okreq == 1){
                                                                //remove from recv list to ok list
                                                                recvr_list[recv_index] = 0;
                                                                ok_list[poll_node] = poll_node;
                                                                num_oks++;
                                                                recv_index++;
                                                                num_receivers--;
                                                        }else if(sendreq == 1){//RECVR
                                                                recv_index++;
                                                        }
					/*		 if(job_next<qsize){
        		                                        trial_div(jobque[job_next].start, jobque[job_next].end, fact_num, myid);
		                                                job_next++;
                		                                dyn_qsize--;

							}*/
pthread_mutex_unlock( &mutex31 );
                                                }
}
						}
					else{

///here last
						  if(job_next<qsize){
                                                                trial_div(jobque[job_next].start, jobque[job_next].end, fact_num, myid);
pthread_mutex_lock( &mutex33 );
                                                                job_next++;
                                                                dyn_qsize--;
pthread_mutex_unlock( &mutex33 );
						}
					}
					npoll++;
				}
				else					//dyn_qsize else
				{
					//do work
					if(job_next<qsize){
						trial_div(jobque[job_next].start, jobque[job_next].end, fact_num, myid);
pthread_mutex_lock( &mutex18 );

                                		job_next++;
                                		dyn_qsize--;
pthread_mutex_unlock( &mutex18 );

					}
				}

			}
			else						//npoll else
			{
				//do work
				if(job_next<qsize){
					trial_div(jobque[job_next].start, jobque[job_next].end, fact_num, myid);
pthread_mutex_lock( &mutex19 );
                        		job_next++;
                        		dyn_qsize--;
pthread_mutex_unlock( &mutex19 );

				}
			}

		}
	//

/*****************************************************************/

//actual work								//polling
/*		for(int ik = 0; ik< qsize; ik++)
		{

			trial_div(jobque[job_next].start, jobque[job_next].end, fact_num, myid);
                        job_next++;
                        dyn_qsize--;

		}
*/
//SCHEDULING - ends	
		cout<<myid<<" :: ##############################################"<<endl<<endl;

	}



	if(ranges != NULL)
                delete [] ranges;
        if(num_of_ranges !=NULL)
                delete [] num_of_ranges;
	
	MPI_Finalize();												// ends MPI
	return 0;

}
