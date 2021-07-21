#include <bits/stdc++.h>
#include <limits.h>
#include <pthread.h>
#include <assert.h>
// #include "stdafx.h"
// #include "mongo/client/dbclient.h"

using namespace std;
#include "../../kv_store/include/read_response_selector.h"
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include "vaccination.h"

#define arrayin(i,n)    for(auto i=0;i<n;i++)
#define loop(i,n)       for(auto i=1;i<=n;i++)
#define ull unsigned long long int
#define ll long long int
#define pri pair<int,int>
#define prl pair<ll,ll>
#define ff first
#define ss second
#define pb push_back
#define all(v)  (v).begin(),(v).end()
#define print(arr)  arrayin(i,n) cout<<arr[i]<<" "; cout<<endl;

const string server = "127.0.0.1:3306";
const string username = "root";
const string password = "codechef";

const int OPS_PER_THREAD = 3;
const int NUM_THREADS = 3;
const int TOTAL_OPS = NUM_THREADS*OPS_PER_THREAD;
const int NUM_OPS = 3;
const int runs = 2;

vector<pri> results(TOTAL_OPS);
vector<set<int>> values(TOTAL_OPS);

int results_size;
Vaccination *v;
sql_Vaccination *s;

void random_operations(vector<int> &list,int tid)
{
    list.pb(tid);

    arrayin(i,OPS_PER_THREAD)
    {
        int id = rand()%NUM_OPS;
        list.pb(id);
    }
}
 
void check_result()
{
    for(auto i : results)
        cout << i.ff << "(" << i.ss << ") ";
    cout << endl;
        // check the result is present within set of results
}

void* run_iteration(void* args)
{
    vector<int> *temp = (vector<int>*)args;
    vector<int> params = *temp;

    int x;
    int id = params[0];

    for(int i=1;i<params.size();i++)
    {
        v->mx.lock();

        if(params[i]==0)
        {
            x = v->getVaccine(id);
        }
        else
        if(params[i]==1)
        {
            x = v->isEmpty(id);
        }
        else
        if(params[i]==2)
        {
            x = v->getCnt(id);
        }

        results[id*NUM_OPS+i-1].ff = x;
        results[id*NUM_OPS+i-1].ss = params[i];

        v->mx.unlock();
    }

    pthread_exit(NULL);
}

void *run_sql_iteration(void *args)
{
    vector<int> *temp = (vector<int>*)args;
    vector<int> params = *temp;

    int x;
    int id = params[0];

    for(int i=1;i<params.size();i++)
    {
        s->mx.lock();

        if(params[i]==0)
        {
            x = s->getVaccine(id);
        }
        else
        if(params[i]==1)
        {
            x = s->isEmpty(id);
        }
        else
        if(params[i]==2)
        {
            x = s->getCnt(id);
        }

        results[id*NUM_OPS+i-1].ff = x;
        results[id*NUM_OPS+i-1].ss = params[i];

        s->mx.unlock();
    }

    pthread_exit(NULL);
}
 
int main(int argc, char const *argv[])
{
    srand(time(NULL));

    int type;
    string consistency_level;

    if(argc==1)
    {
        cout << "Improper input" << endl;
        return -1;
    }
    else
    {
        string db = argv[1];
        consistency_level = argv[2];

        if(db=="0")
        {
            type = 0;       
        }
        else
        {
            type = 1;
        }
    }

    vector<int> thread_functions[NUM_THREADS];
    results_size = 0;

    arrayin(i,NUM_THREADS)
    {
        random_operations(thread_functions[i],i);
        results_size += thread_functions[i].size()-1;
    }

    if(type==0)
    {
        ofstream fout("monkey_db_log.txt");

        arrayin(i,runs)
        {
            mockdb::read_response_selector<string, std::pair<int, long>> *consistency_check;

            if (consistency_level == "causal")
                consistency_check = new mockdb::causal_read_response_selector<string, std::pair<int, long>>();
            else if (consistency_level == "linear")
                consistency_check = new mockdb::linearizable_read_response_selector<string, std::pair<int, long>>();
            else
                consistency_check = new mockdb::causal_read_response_selector<string, std::pair<int, long>>();

            mockdb::kv_store<string, std::pair<int, long>> *store = new mockdb::kv_store<string, std::pair<int, long>>(consistency_check);
            consistency_check->init_consistency_checker(store);

            v = new Vaccination(store);

            pthread_t threads[NUM_THREADS];
            pthread_attr_t attr;
            pthread_attr_init(&attr);

            arrayin(i,NUM_THREADS)
            {
                pthread_create(&threads[i],&attr,run_iteration,(void *)&thread_functions[i]);
            }

            arrayin(i,NUM_THREADS)
            {
                pthread_join(threads[i],NULL);
            }

            // check_result();
            for(auto i : results)
                fout << i.ff << "(" << i.ss << ") ";
            fout << endl;

            arrayin(i,results.size())
            {
                values[i].insert(results[i].ff);
            }

            cout << "Successfully completed" << endl;
        }

        fout.close();

        fout.open("monkey_db_set_of_values.txt");

        int k=0;
        arrayin(i,NUM_THREADS)
        {
            arrayin(j,NUM_OPS)
            {
                fout << "T" << i << "_" << "O" << j << " : {";

                for(auto val : values[k])
                {
                    fout << val << " ";
                }

                fout << "}" << endl;
                k++;
            }
        }

        fout.close();
    }
    else
    {
        ofstream fout("mysql_db_log.txt");

        arrayin(i,runs)
        {         
            sql::Driver *driver;
            sql::Connection *con;  

            try{
                driver = get_driver_instance();
                con = driver->connect(server,username,password);
            } 
            catch(sql::SQLException e)
            {
                cout << "Could not connect to server. Error message: " << e.what() << endl;
                system("pause");
                exit(1);
            }

            s = new sql_Vaccination(con);
            // s->getVaccine(1);

            pthread_t threads[NUM_THREADS];
            pthread_attr_t attr;
            pthread_attr_init(&attr);

            arrayin(i,NUM_THREADS)
            {
                pthread_create(&threads[i],&attr,run_sql_iteration,(void *)&thread_functions[i]);
            }

            arrayin(i,NUM_THREADS)
            {
                pthread_join(threads[i],NULL);
            }

            // check_result();
            for(auto i : results)
                fout << i.ff << "(" << i.ss << ") ";
            fout << endl;

            arrayin(i,results.size())
            {
                values[i].insert(results[i].ff);
            }

            cout << "Successfully completed" << endl;
            delete s;
        }

        fout.close();

        fout.open("mysql_db_set_of_values.txt");

        int k=0;
        arrayin(i,NUM_THREADS)
        {
            arrayin(j,NUM_OPS)
            {
                fout << "T" << i << "_" << "O" << j << " : {";

                for(auto val : values[k])
                {
                    fout << val << " ";
                }

                fout << "}" << endl;
                k++;
            }
        }

        fout.close();    
    }

    // g++ main.cpp -pthread -lmysqlcppconn

    return 0;
}
