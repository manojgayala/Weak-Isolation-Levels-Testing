import java.sql.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.Semaphore;

public class Iteration extends Thread{

    Connection con;
    int[] params;
    Semaphore mutex;
    static int TOTAL_OPS;
    static int[][] results = new int[TOTAL_OPS][2];;
    static String server = "jdbc:mysql://localhost:3306/project?autoReconnect=true&useSSL=false";
    static String username = "root";
    static String password = "codechef";

    public Iteration(int[] args,int total){
        con = null;
        try{
            con = DriverManager.getConnection(
                    server,
                    username,
                    password
                );
        
        }catch(SQLException Ex){
            System.out.println("Connection denied!!");
        }

        params = args;
        mutex = new Semaphore(1);
        TOTAL_OPS = total;
    }

    public void run(){

        int x = 0;
        int id = params[0];

        for(int k=1;k<params.length;k++)
        {
            mutex.acquire();

            if(params[k]==0)
            {
                x = getVaccine(con,id);
            }
            else
            if(params[k]==1)
            {
                x = isEmpty(con,id);
            }
            else
            if(params[k]==2)
            {
                x = getCnt(con,id);
            }

            results[id*NUM_OPS+k-1][0] = x;
            results[id*NUM_OPS+k-1][0] = params[k];

            mutex.release();
        }
    }

    public static void summarize()
    {
        FileWriter fout = null;

        try{
            for(int j=0;j<results.length;j++)
                fout.write("" + results[j][0] + "(" + results[j][1] + ") ");  
            fout.write("\n");
            fout.close();
        }catch(IOException e){
            System.out.println("Error");
        }
    }
}

public class Sql {

    static int OPS_PER_THREAD = 3;
    static int NUM_THREADS = 3;
    static int TOTAL_OPS = NUM_THREADS*OPS_PER_THREAD;
    static int NUM_OPS = 3;
    static int NUM_VACCINES = 10;
    static int runs = 2;
    static List<Set<Integer>> values;

    public static void random_operations(int[] list,int tid)
    {
        list[0] = tid;
        Random rand = new Random();

        for(int i=0;i<OPS_PER_THREAD;i++)
        {
            int id = rand.nextInt(NUM_OPS);
            list[i+1] = id;
        }
    }

    public static void main(String[] args) throws SQLException {

    int type;
    String consistency_level;
    System.out.println(args.length);
    if(args.length==0)
    {
        System.out.println("Improper input!");
        return;
    }
    else
    {
        String db = args[0];
        consistency_level = args[1];

        if(db.equals("0"))
        {
            type = 0;       
        }
        else
        {
            type = 1;
        }
    }

    int[][] thread_functions = new int[NUM_THREADS][OPS_PER_THREAD+1];

    for(int i=0;i<NUM_THREADS;i++)
    {   
        random_operations(thread_functions[i],i);
    }

    // for(int i=0;i<NUM_THREADS;i++)
    // {
    //     for(int j=0;j<=OPS_PER_THREAD;j++)
    //         System.out.println(thread_functions[i][j]);
    // }

    results = new int[TOTAL_OPS][2];
    values = new ArrayList<Set<Integer>>();

    for(int i=0;i<TOTAL_OPS;i++)
        values.add(new HashSet<Integer>());

    if(type==0)
    {
        FileWriter fout = null;
        try{
            fout = new FileWriter("monkey_db_log.txt");
        }catch(IOException e){
            System.out.println("Error");
        }

        for(int i=0;i<runs;i++)
        {
            // mockdb::read_response_selector<string, std::pair<int, long>> *consistency_check;

            // if (consistency_level == "causal")
            //     consistency_check = new mockdb::causal_read_response_selector<string, std::pair<int, long>>();
            // else if (consistency_level == "linear")
            //     consistency_check = new mockdb::linearizable_read_response_selector<string, std::pair<int, long>>();
            // else
            //     consistency_check = new mockdb::causal_read_response_selector<string, std::pair<int, long>>();

            // mockdb::kv_store<string, std::pair<int, long>> *store = new mockdb::kv_store<string, std::pair<int, long>>(consistency_check);
            // consistency_check->init_consistency_checker(store);

            for(int j=0;j<NUM_THREADS;j++)
            {
                final int[] params = new int[OPS_PER_THREAD+1];
                for(int k=0;k<OPS_PER_THREAD+1;k++)
                    params[k]=thread_functions[j][k];

                Runnable r = ()->{

                    int x = 0;
                    int id = params[0];

                    for(int k=1;k<params.length;k++)
                    {
                        mutex.acquire();

                        // if(params[i]==0)
                        // {
                        //     x = v.getVaccine(id);
                        // }
                        // else
                        // if(params[i]==1)
                        // {
                        //     x = v.isEmpty(id);
                        // }
                        // else
                        // if(params[i]==2)
                        // {
                        //     x = v.getCnt(id);
                        // }

                        results[id*NUM_OPS+i-1][0] = x;
                        results[id*NUM_OPS+i-1][0] = params[i];

                        mutex.release();
                    }
                }; 

                Thread t = new Thread(r);
                t.start();
            }

            // check_result();
            try{
                for(int j=0;j<results.length;j++)
                    fout.write("" + results[j][0] + "(" + results[j][1] + ") ");  
                fout.write("\n");
            }catch(IOException e){
                System.out.println("Error");
            }

            for(int j=0;j<results.length;j++)
            {
                values.get(j).add(results[j][0]);
            }

            System.out.println("Successful");
        }

        try{
            fout.close();
        }catch(IOException e){
            System.out.println("Error");
        }
    }
    else
    {
        try{
            fout = new FileWriter("mysql_db_log.txt");
        }catch(IOException e){
            System.out.println("Error");
        }

        for(int i = 0; i < runs; ++i) { 
            
                for(int j=0;j<NUM_THREADS;j++)
                {                    
                    Iteration t = new Iteration(thread_functions[j],TOTAL_OPS);
                    t.start();
                }

                for(int j=0;j<NUM_THREADS;j++)
                {                    
                    
                }

                Iteration.summarize();

                for(int j=0;j<results.length;j++)
                {
                    values.get(j).add(results[j][0]);
                }

                System.out.println("Successful");
            }
        

        try{
            fout.close();
        }catch(IOException e){
            System.out.println("Error");
        }

        try{
            fout = new FileWriter("mysql_db_set_of_values.txt");
        }catch(IOException e){
            System.out.println("Error");
        }

        int k=0;
        for(int i=0;i<NUM_THREADS;i++)
        {
            for(int j=0;j<NUM_OPS;j++)
            {
                fout.write("T"+i+"_"+"O"+j+" :{");
                for(int v : values.get(k))
                {
                    fout.write(""+v+" ");   
                }
    
                fout.write("}\n");
                k++;
            }
        }

        try{
            fout.close();
        }catch(IOException e){
            System.out.println("Error");
        }
    }

    }

    static int getVaccine(Connection con,int tid)
    {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT X FROM STORE;");
        int vaccines;
        while(rs.next())
            vaccines = rs.getInt(1);

        if(vaccines>0)
        {
            PreparedStatement prep_stmt = con.prepareStatement("UPDATE STORE SET X = ?;");
            vaccines--;
            prep_stmt.setInt(1,vaccines);
            prep_stmt.execute();
        
            return 1;
        }

        return 0;
    }

    static int isEmpty(Connection con,int tid)
    {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT X FROM STORE;");
        int vaccines;
        while(rs.next())
            vaccines = rs.getInt(1);

        if(vaccines>0)
            return 0;

        return 1;
    }

    static int getCnt(Connection con,int tid)
    {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT X FROM STORE;");
        int vaccines;
        while(rs.next())
            vaccines = rs.getInt(1);

        return vaccines;
    }
}
