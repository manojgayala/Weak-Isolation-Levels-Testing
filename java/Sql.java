import java.sql.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.Semaphore;

class Sql {

    static int OPS_PER_THREAD = 3;
    static int NUM_THREADS = 3;
    static int TOTAL_OPS = NUM_THREADS*OPS_PER_THREAD;
    static int NUM_OPS = 3;
    static int runs;
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
            Iteration.name = "monkey";       
        }
        else
        {
            Iteration.name = "mysql";
        }
    }

    runs = Integer.parseInt(args[2]);
    int[][] thread_functions = new int[NUM_THREADS][OPS_PER_THREAD+1];

    for(int i=0;i<NUM_THREADS;i++)
    {   
        random_operations(thread_functions[i],i);
    }

    values = new ArrayList<Set<Integer>>();

    for(int i=0;i<TOTAL_OPS;i++)
        values.add(new HashSet<Integer>());

    for(int i=0;i<NUM_THREADS;i++)
    {
        for(int j=0;j<OPS_PER_THREAD+1;j++)
        {
            System.out.print(thread_functions[i][j]+" ");
        }

        System.out.print("\n");
    }

    for(int i = 0; i < runs; ++i) { 
        
            Iteration.Setter(TOTAL_OPS,OPS_PER_THREAD);
            for(int j=0;j<NUM_THREADS;j++)
            {                    
                Iteration t = new Iteration(thread_functions[j]);
                t.start();
            }

            Iteration.summarize();

            for(int j=0;j<Iteration.results.length;j++)
            {
                values.get(j).add(Iteration.results[j][0]);
            }

            System.out.println("Successful Run!!");
        }


    try{
        Iteration.fout.close();
        
    }catch(IOException e){
        System.out.println("Error");
    }
    
    FileWriter fout = null;

    try{
        fout = new FileWriter(Iteration.name+"_db_set_of_values.txt");
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

        fout.close();
    }catch(IOException e){
        System.out.println("Error");
    }

    }
}

class Iteration extends Thread{

    Connection con;
    int[] params;
    Semaphore mutex;
    static FileWriter fout;
    static int TOTAL_OPS;
    static int OPS_PER_THREAD;
    static int NUM_VACCINES = 10;
    static int[][] results;
    static String server = "jdbc:mysql://localhost:3306/project?allowPublicKeyRetrieval=true&autoReconnect=true&useSSL=false";
    static String username = "test";
    static String password = "newpassword";
    static String name;
// /project?autoReconnect=true&useSSL=false
    public static void Setter(int total,int ops)
    {
        TOTAL_OPS = total;
        OPS_PER_THREAD = ops;
        results = new int[TOTAL_OPS][2];

        Connection con = null;
        try{
            con = DriverManager.getConnection(
                    server,
                    username,
                    password
                );
            System.out.println("Connection established....");
            con.setAutoCommit(false);

            Statement create_stmt = con.createStatement();
            create_stmt.execute("CREATE TABLE if not exists STORE (X int);");
            Statement del_stmt = con.createStatement();
            del_stmt.execute("DELETE FROM STORE;");

            PreparedStatement insert_stmt = con.prepareStatement("INSERT INTO STORE(X) VALUES ("+NUM_VACCINES+");");
            // insert_stmt.setInt(1,NUM_VACCINES);
            insert_stmt.execute();
            con.commit();
            con.close();
        }catch(SQLException e){
            System.out.println("Connection Failed!!");
            e.printStackTrace();
        }       

        try{
            fout = new FileWriter(name+"_db_log.txt");
        }catch(IOException e){
            System.out.println("Error");
        }
    }

    public Iteration(int[] args){
        con = null;
        try{
            con = DriverManager.getConnection(
                    server,
                    username,
                    password
                );
        
            con.setAutoCommit(false);
        }catch(SQLException e){
            System.out.println("Connection denied!!");
            e.printStackTrace();
        }

        params = args;
        mutex = new Semaphore(1);
    }

    public void run(){

        int x = 0;
        int id = params[0];

        try{
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

                results[id*OPS_PER_THREAD+k-1][0] = x;
                results[id*OPS_PER_THREAD+k-1][1] = params[k];

                mutex.release();
            }  
        }catch(InterruptedException e){
            System.out.println("Error in Running thread!!");
        }
        
    }

    static int getVaccine(Connection con,int tid)
    {
        try{
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT X FROM STORE;");
            int vaccines=0;
            while(rs.next())
                vaccines = rs.getInt(1);

            if(vaccines>0)
            {
                PreparedStatement prep_stmt = con.prepareStatement("UPDATE STORE SET X = "+--vaccines+";");
                // vaccines--;
                // prep_stmt.setInt(1,vaccines);
                prep_stmt.execute();
            
                con.commit();
                return 1;
            }

            con.commit();
            return 0;
        }catch(SQLException Ex){
            System.out.println("Connection denied!!");
            return -1;
        }
        
    }

    static int isEmpty(Connection con,int tid)
    {
        try{
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT X FROM STORE;");
            int vaccines=0;
            while(rs.next())
                vaccines = rs.getInt(1);

            con.commit();

            if(vaccines>0)
                return 0;
            
            return 1;
        }catch(SQLException Ex){
            System.out.println("IsEmpty error!!");
            return -1;
        }
        
    }

    static int getCnt(Connection con,int tid)
    {
        try{
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT X FROM STORE;");
            int vaccines=0;
            while(rs.next())
                vaccines = rs.getInt(1);
            con.commit();
            return vaccines;
        }catch(SQLException Ex){
            System.out.println("GetCnt error!!");
            return -1;
        }
    }

    public static void summarize()
    {
        try{
            for(int j=0;j<results.length;j++)
                fout.write("" + results[j][0] + "(" + results[j][1] + ") ");  
            fout.write("\n");
        }catch(IOException e){
            System.out.println("Error");
        }
    }
}

// javac Sql.java
// java -cp :mysql-connector-java-5.1.47.jar Sql 1 causal 1
