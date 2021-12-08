import java.sql.*;
import java.util.*;
import java.io.*;                                     // libraries have been imported
import java.util.concurrent.*;

/*
    This test case contains 2 threads and each thread has 3 operations.
    The first thread has operations of SelectAll, Update, SelectAll
    The second thread has operations of Insert, Insert, SelectAll
    The update condition is set in such a way that the inserted rows are modified
*/


public class PRT1 {                      // Update vs Insert

    static int OPS_PER_THREAD = 3;
    static int NUM_THREADS = 2;
    static int TOTAL_OPS = NUM_THREADS*OPS_PER_THREAD;
    static int NUM_OPS = 3;
    static int runs;
    static ArrayList<Set<Integer>> values = new ArrayList<Set<Integer>>();        

    public static void random_operations(int[] list,int tid)
    {
        list[0] = tid;
        
        if(tid==0)
        {
            list[1] = 0;                // selects all rows
            list[2] = 2;                // updates some rows
            list[3] = 0;                // selects all rows
        }
        else
        if(tid==1)
        {
            list[1] = 1;                // inserts new row
            list[2] = 1;                // inserts new row
            list[3] = 0;                // selects all rows
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
                                                    // the databases are encoded
        if(db.equals("0"))
        {
            Iteration.name = "monkey"; 
            Iteration.server = "jdbc:postgresql://localhost/project?user=test&password=newpassword&ssl=false";      
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
            Iteration.server = "jdbc:postgresql://localhost/project?user=test&password=newpassword&ssl=false";      
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
    int[][] thread_functions = new int[NUM_THREADS][OPS_PER_THREAD+1];

    for(int i=0;i<NUM_THREADS;i++)
    {   
        random_operations(thread_functions[i],i);
    }

    try{
        Iteration.fout = new FileWriter("../text/"+Iteration.name+"_PRT1_iterations.txt");
    }catch(IOException e){
        System.out.println("Error");
    }

    for(int i=0;i<TOTAL_OPS;i++)
    {
        values.add(new HashSet<Integer>());
    }

    for(int i = 0; i < runs; ++i) { 
        
        Iteration.Setter(TOTAL_OPS,OPS_PER_THREAD);
        List<Iteration> threads = new ArrayList<Iteration>();

        for(int j=0;j<NUM_THREADS;j++)
        {                    
            Iteration t = new Iteration();
            t.params = new int[OPS_PER_THREAD+1];

            for(int k=0;k<=OPS_PER_THREAD;k++)
                t.params[k] = thread_functions[j][k];

            threads.add(t);
            t.start();
        }

        try{     
            for(Iteration t : threads)
                t.join();                                       

                Iteration.summarize();

                for(int j=0;j<Iteration.results.length;j++)
                {
                    values.get(j).add(Iteration.results[j][0]);                 // the read from values are stored here for each operation
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
        fout = new FileWriter("../text/"+Iteration.name+"_PRT1_summary.txt");
        int k=0;
        for(int i=0;i<NUM_THREADS;i++)
        {
            for(int j=0;j<OPS_PER_THREAD;j++)
            {
                fout.write("T"+i+"_"+"O"+j+" (");

                int op = Iteration.results[k][1];
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

                fout.write(") :{");
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
    static FileWriter fout;
    static Connection common_con;
    static int TOTAL_OPS;
    static int OPS_PER_THREAD;
    static int NUM_VACCINES = 10;
    static int[][] results;
    static String server;
    static String username = "test";
    static String password = "newpassword";
    static String name;


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

        }catch(SQLException e){
            System.out.println("Connection Failed!!");
            e.printStackTrace();
        } 

        try{
            Statement create_stmt = con.createStatement();
            create_stmt.execute("CREATE TABLE STUDENTS (NAME VARCHAR(20), PLACE VARCHAR(20), MARKS INTEGER);");
            con.commit();
        }catch(SQLException e){}
            
        try{
            Statement del_stmt = con.createStatement();
            del_stmt.execute("DELETE FROM STUDENTS;");  // the database is setup
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

        String[] l = new String[3];
        try{
        con.setAutoCommit(false);
        PreparedStatement isolation_stmt = con.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");        // repeatable read isolation level
        isolation_stmt.execute();

                                if(id==0)
                                {
                                    try{
                                    Thread.sleep(200);
                                }catch(InterruptedException e){
                                    e.printStackTrace();
                                }}

            for(int k=1;k<params.length;k++)
            {
                if(params[k]==0)
                {
                    x = Studentcnt(con,id);
                }
                else
                if(params[k]==1)
                {
                    if(id==1)
                    {
                        if(k==1)
                        {
                            l[0] = "Manoj";
                            l[1] = "Visakhapatnam";
                            l[2] = "95";
                        }
                        else
                        {                                   // specific rows are inserted here
                            l[0] = "Deepu";
                            l[1] = "Visakhapatnam";
                            l[2] = "75";
                        }
                    }

                    x = AddStudent(con,id,l);
                }
                else
                if(params[k]==2)
                {
                    l[0] = "Visakhapatnam";                // update condition
                    l[1] = "90";

                    x = UpdateStudent(con,id,l);
                }

                results[id*OPS_PER_THREAD+k-1][0] = x;
                results[id*OPS_PER_THREAD+k-1][1] = params[k];
            }  

        con.commit();
        con.close();

        }catch(SQLException e){
            System.out.println("Error in Running thread!!");
        }
        
    }

    static int Studentcnt(Connection con,int tid)                   // selects the count of students in table
    {
        try{
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM STUDENTS;");
            
            int cnt=0;
            while(rs.next())
                cnt = rs.getInt(1);

            return cnt;
        }catch(SQLException Ex){
            System.out.println("Studentcnt error!!");
            return -1;
        }
        
    }

    static int AddStudent(Connection con,int tid,String[] l)         // adds new student to the table
    {
        try{
            PreparedStatement insert_stmt = con.prepareStatement("INSERT INTO STUDENTS VALUES ('"+l[0]+"','"+l[1]+"',"+l[2]+");");
            insert_stmt.execute();

            return 1;
        }catch(SQLException Ex){
            System.out.println("AddStudent error!!");
            return -1;
        }
        
    }

    static int UpdateStudent(Connection con,int tid,String[] l)      // updates the student row
    {   
        try{
            PreparedStatement prep_stmt = con.prepareStatement("UPDATE STUDENTS SET MARKS = "+l[1]+" WHERE PLACE = '"+l[0]+"';");
            prep_stmt.execute();
        
            return 1;
        }catch(SQLException e){
            System.out.println("UpdateStudent error!!");
            e.printStackTrace();
            return -1;
        }
    }

    public static void summarize()                                    // summarizes the read from values for each iteration
    {
        try{
            for(int j=0;j<results.length;j++)
            {
                fout.write("" + results[j][0] + "(");

                int op = results[j][1];
                if(op == 0)
                {
                    fout.write("Studentcnt");
                }
                else
                if(op == 1)
                {
                    fout.write("AddStudent");
                }
                else
                if(op == 2)
                {
                    fout.write("UpdateStudent");
                }

                fout.write(") \n");  
            }

            fout.write("\n");
        }catch(IOException e){
            System.out.println("Summary Error");
        }
    }
}