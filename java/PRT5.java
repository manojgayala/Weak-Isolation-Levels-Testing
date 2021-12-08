import java.sql.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;

/*
    This test case contains 2 threads and 2 operations in first thread and 4 operations in second thread
    The first thread contains Insert, SelectAll
    The second thread contains Insert, SelectAll, DeleteSpecific, SelectAll
*/

public class PRT5 {                    // Insert vs Delete

    static int MAX_OPS_PER_THREAD = 4;
    static int NUM_THREADS = 2;
    static int NUM_OPS = 5;
    static int runs;
    static ArrayList<Set<String>> values = new ArrayList<Set<String>>();

    public static void random_operations(int[] list,int tid)
    {
        list[0] = tid;
        
        if(tid==0)
        {
            list[1] = 2;          
            list[2] = 1;            // Insert 
            list[3] = 0;            // SelectAll
        }
        else
        if(tid==1)
        {
            list[1] = 4;
            list[2] = 1;            // Insert
            list[3] = 0;            // SelectAll
            list[4] = 4;            // DeleteSpecific
            list[5] = 0;            // SelectAll
        }
    }

public static void main(String[] args) throws SQLException {

    int type;

    if(args.length==0)
    {
        System.out.println("Improper input!");
        return;
    }
    else
    {
        String db = args[0];

        if(db.equals("0"))
        {
            Iteration.name = "monkey"; 
            Iteration.server = "jdbc:mysql://localhost:3306/project?allowPublicKeyRetrieval=true&autoReconnect=true&useSSL=false";
        }
        else
        if(db.equals("1"))
        {
            Iteration.name = "mysql";
            Iteration.server = "jdbc:mysql://localhost:3306/project?allowPublicKeyRetrieval=true&autoReconnect=true&useSSL=false";
        }
        else
        if(db.equals("2"))
        {
            Iteration.name = "pg";
            Iteration.server = "jdbc:postgresql://localhost:5432/project?user=test&password=newpassword&ssl=false";      
        }
        else
        if(db.equals("3"))
        {
            Iteration.name = "sqlsv";
            Iteration.server = "jdbc:sqlserver://localhost:1433;databaseName=project;user=sa;password=Manoj@123;ssl=false";      
            Iteration.username = "sa";
            Iteration.password = "Manoj@123";
        }
    }

    runs = Integer.parseInt(args[1]);
    int[][] thread_functions = new int[NUM_THREADS][MAX_OPS_PER_THREAD+2];

    for(int i=0;i<NUM_THREADS;i++)
    {   
        random_operations(thread_functions[i],i);
    }

    try{
        Iteration.fout = new FileWriter("../text/"+Iteration.name+"_PRT5_iterations.txt");
    }catch(IOException e){
        System.out.println("Error");
    }

    for(int i=0;i<NUM_THREADS*MAX_OPS_PER_THREAD;i++)
    {
        values.add(new HashSet<String>());
    }

    for(int i = 0; i < runs; ++i) { 
        
        Iteration.Setter(NUM_THREADS,MAX_OPS_PER_THREAD);
        List<Iteration> threads = new ArrayList<Iteration>();

        for(int j=0;j<NUM_THREADS;j++)
        {                    
            Iteration t = new Iteration();
            t.params = new int[MAX_OPS_PER_THREAD+2];

            for(int k=0;k<MAX_OPS_PER_THREAD+2;k++)
                t.params[k] = thread_functions[j][k];

            threads.add(t);
            t.start();
        }

        try{     
            for(Iteration t : threads)
                t.join();   

                Iteration.summarize();
                int ptr = 0;
                for(int j=0;j<Iteration.results.length;j++)
                {
                    int op = Iteration.results[j][1];
                    if(op == -1)
                        continue;

                    if(op < 5)
                        values.get(j).add(Integer.toString(Iteration.results[j][0]));
                    else
                    {
                        for(int l=0;l<Iteration.results[j][0];l++)
                            values.get(j).add(Iteration.result_vals[ptr++]);
                    }
                }

                System.out.println("Successful Run!!");

        }catch(InterruptedException e){
            System.out.println("Error in threads joining");
            e.printStackTrace();
        }
    }

    try{
        Iteration.fout.close();
        
    }catch(IOException e){
        System.out.println("Error");
    }
    
    FileWriter fout = null;

    try{
        fout = new FileWriter("../text/"+Iteration.name+"_PRT5_summary.txt");
        int k=0;
        for(int i=0;i<NUM_THREADS;i++)
        {
            for(int j=0;j<MAX_OPS_PER_THREAD;j++,k++)
            {
                int op = Iteration.results[k][1];
                
                if(op == -1)
                    continue;

                fout.write("T"+i+"_"+"O"+j+" (");

                if(op == 0)
                {
                    fout.write("Read");
                }
                else
                if(op == 1)
                {
                    fout.write("Write");
                }
                else
                if(op == 2)
                {
                    fout.write("Update");
                }
                else
                if(op == 3)
                {
                    fout.write("DeleteAll");
                }
                else
                if(op == 4)
                {
                    fout.write("DeleteSpec");
                }
                else
                {
                    fout.write("SelectName");
                }

                fout.write(") :{");
                for(String v : values.get(k))
                {
                    fout.write(""+v+" ");   
                }

                fout.write("}\n");
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
    static FileWriter fout;
    static Connection common_con;
    static int NUM_THREADS;
    static int MAX_OPS_PER_THREAD;
    static int NUM_VACCINES = 10;
    static int[][] results;
    static String[] result_vals;
    static int ptr = 0;
    static String server;
    static String username = "test";
    static String password = "newpassword";
    static String name;


    public static void Setter(int thread_cnt,int ops)
    {
        NUM_THREADS = thread_cnt;
        MAX_OPS_PER_THREAD = ops;
        results = new int[MAX_OPS_PER_THREAD*NUM_THREADS][2];
        result_vals = new String[1000];

        for(int i=0;i<MAX_OPS_PER_THREAD*NUM_THREADS;i++)
            results[i][0] = results[i][1] = -1;
        for(int i=0;i<1000;i++)
            result_vals[i] = "";

        Connection con = null;
        try{
            con = DriverManager.getConnection(
                    server,
                    username,
                    password
                );

            System.out.println("Connection established....");
            con.setAutoCommit(false);

        }catch(SQLException e){
            System.out.println("Connection Failed!!");
            e.printStackTrace();
        } 

        try{
            Statement create_stmt = con.createStatement();
            create_stmt.execute("CREATE TABLE PEOPLE (NAME VARCHAR(20),FRIENDS INTEGER);");
            con.commit();
        }catch(SQLException e){}
            
        try{
            Statement del_stmt = con.createStatement();
            del_stmt.execute("DELETE FROM PEOPLE;");
            con.commit();
            con.close();
        }catch(SQLException e){}
    }

    public Iteration(){
        con = null;
        try{
            con = DriverManager.getConnection(
                    server,
                    username,
                    password
                );

        System.out.println("Iteration Connection established....");
        }catch(SQLException e){
            System.out.println("Connection denied!!");
            e.printStackTrace();
        }
    }

    public void run(){

        int x = 0;
        int id = params[0];
        int num_ops = params[1];
        int ptr = 0;

        try{
        con.setAutoCommit(false);
        PreparedStatement isolation_stmt = con.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
        isolation_stmt.execute();

            if(id==1)
            {
                try{
                Thread.sleep(125);
            }catch(InterruptedException e){
                e.printStackTrace();
            }}

            for(int k=2;k<num_ops+2;k++)
            {
                if(params[k]==0)
                {
                    x = Studentcnt(con,id);
                }
                else
                if(params[k]==1)
                {
                    String name = "";
                    int fnds = 0;
                    if(id==0)
                    {
                        name = "Manoj";
                    }
                    else
                    {
                        name = "Manoj**";
                    }

                    x = AddStudent(con,id,name,fnds);
                }
                else
                if(params[k]==2)
                {
                    String name = "Manoj**";
                    String fnds = "9";

                    x = UpdateStudent(con,id,name,fnds);
                }
                else
                if(params[k]==3)
                {
                    x = DeleteStudent(con,id);
                }
                else
                if(params[k]==4)
                {
                    String name = "Manoj";
                    x = DeleteSpecificStudent(con,id,name);
                }
                else
                {
                    ArrayList<String> res = StudentName(con,id);
                    for( String n: res)
                        result_vals[ptr++] = n;

                    x = res.size();
                }

                results[id*MAX_OPS_PER_THREAD+k-2][0] = x;
                results[id*MAX_OPS_PER_THREAD+k-2][1] = params[k];
            }  

        con.commit();
        con.close();

        }catch(SQLException e){
            System.out.println("Error in Running thread!!");
            e.printStackTrace();
        }
        
    }

    static int Studentcnt(Connection con,int tid)
    {
        try{
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM PEOPLE;");
            
            int cnt=0;
            while(rs.next())
                cnt = rs.getInt(1);

            return cnt;
        }catch(SQLException e){
            System.out.println("Studentcnt error!!");
            e.printStackTrace();
            return -1;
        }
        
    }

    static ArrayList<String> StudentName(Connection con,int tid)
    {
        ArrayList<String> res = new ArrayList<String>();
        try{
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT NAME FROM PEOPLE;");

            while(rs.next())
            {
                res.add(rs.getString(1));
            }

            return res;
        }catch(SQLException e){
            System.out.println("StudentName error!!");
            e.printStackTrace();
            return res;
        }
    }

    static int AddStudent(Connection con,int tid,String name,int f)
    {
        try{
            PreparedStatement insert_stmt = con.prepareStatement("INSERT INTO PEOPLE VALUES ('"+name+"',"+f+");");
            insert_stmt.execute();

            return 1;
        }catch(SQLException e){
            System.out.println("AddStudent error!!");
            e.printStackTrace();
            return -1;
        }
        
    }

    static int UpdateStudent(Connection con,int tid,String name,String f)
    {
        try{
            PreparedStatement prep_stmt = con.prepareStatement("UPDATE PEOPLE SET FRIENDS = "+f+" WHERE NAME = '"+name+"';");
            prep_stmt.execute();
        
            return 1;
        }catch(SQLException e){
            System.out.println("UpdateStudent error!!");
            e.printStackTrace();
            return -1;
        }
    }

    static int DeleteStudent(Connection con,int tid)
    {
        try{
            Statement del_stmt = con.createStatement();
            del_stmt.execute("DELETE FROM PEOPLE;");
            con.commit();

            return 1;
        }catch(SQLException e){
            System.out.println("DeleteStudent error!!");
            e.printStackTrace();
            return -1;
        }
    }

    public int DeleteSpecificStudent(Connection con,int tid,String name)
    {
        try{
            Statement del_stmt = con.createStatement();
            del_stmt.execute("DELETE FROM PEOPLE WHERE NAME = '"+name+"';");
            con.commit();

            return 1;

        }catch(SQLException e){
            System.out.println("DeleteSpecificStudent error!!");
            e.printStackTrace();
            return -1;
        }
    }

    public static void summarize()
    {
        try{
            int ptr = 0;
            for(int j=0;j<results.length;j++)
            {
                int op = results[j][1];
                if(op == -1)
                    continue;

                if(op == 0)
                {
                    fout.write("" + results[j][0] + "(");
                    fout.write("Studentcnt");
                }
                else
                if(op == 1)
                {
                    fout.write("" + results[j][0] + "(");
                    fout.write("AddStudent");
                }
                else
                if(op == 2)
                {
                    fout.write("" + results[j][0] + "(");
                    fout.write("UpdateStudent");
                }
                else
                if(op == 3)
                {
                    fout.write("" + results[j][0] + "(");
                    fout.write("DeleteStudent");
                }
                else
                if(op == 4)
                {
                    fout.write("" + results[j][0] + "(");
                    fout.write("DeleteSpecificStudent");
                }
                else
                {
                    fout.write("{");
                    
                    for(int k=0;k<results[j][0];k++)
                    {
                        fout.write(result_vals[ptr++]+" ");
                    }

                    fout.write("}(");
                    fout.write("SelectName");
                }

                fout.write(") \n");  
            }

            fout.write("\n");
        }catch(IOException e){
            System.out.println("Summary Error");
            e.printStackTrace();
        }
    }
}