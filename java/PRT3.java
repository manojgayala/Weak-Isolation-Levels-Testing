import java.sql.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;

/*
    This test case contains 3 threads and all three contain 3 operations each.
    The first thread contains Insert, SelectAll, Insert
    The second thread contains all DeleteAll operations
    The third thread contains SelectAll, Update, SelectAll
    The update modifies the inserted rows and delete simply deletes all rows of table
*/

public class PRT3 {                  // 3 threads; Insert vs Delete vs Update

    static int OPS_PER_THREAD = 3;
    static int NUM_THREADS = 3;
    static int TOTAL_OPS = NUM_THREADS*OPS_PER_THREAD;
    static int NUM_OPS = 4;
    static int runs;
    static ArrayList<Set<Integer>> values = new ArrayList<Set<Integer>>();

    public static void random_operations(int[] list,int tid)
    {
        list[0] = tid;
        
        if(tid==0)
        {
            list[1] = 1;                // Insert
            list[2] = 0;                // SelectAll
            list[3] = 1;                // Insert
        }                              
        else
        if(tid==1)
        {
            list[1] = 3;
            list[2] = 3;                // DeleteAll
            list[3] = 3;
        }
        else
        if(tid==2)
        {
            list[1] = 0;                // SelectAll
            list[2] = 2;                // Update
            list[3] = 0;                // SelectAll
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
    }

    runs = Integer.parseInt(args[1]);
    int[][] thread_functions = new int[NUM_THREADS][OPS_PER_THREAD+1];

    for(int i=0;i<NUM_THREADS;i++)
    {   
        random_operations(thread_functions[i],i);
    }

    try{
        Iteration.fout = new FileWriter("../text/"+Iteration.name+"_PRT3_iterations.txt");
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
                    values.get(j).add(Iteration.results[j][0]);
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
        fout = new FileWriter("../text/"+Iteration.name+"_PRT3_summary.txt");
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
                else
                if(op == 3)
                {
                    fout.write("Delete");
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

            Statement create_stmt = con.createStatement();
            create_stmt.execute("CREATE TABLE STUDENTS (NAME VARCHAR(20), PLACE VARCHAR(20), MARKS INTEGER);");
            con.commit();

            Statement del_stmt = con.createStatement();
            del_stmt.execute("DELETE FROM STUDENTS;");
            con.commit();

            con.close();
        }catch(SQLException e){
            System.out.println("Connection Failed!!");
            e.printStackTrace();
        }       

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
        PreparedStatement isolation_stmt = con.prepareStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
        isolation_stmt.execute();

            if(id==2)
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
                        {
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
                    l[0] = "Visakhapatnam";
                    l[1] = "90";

                    x = UpdateStudent(con,id,l);
                }
                else
                if(params[k]==3)
                {
                    x = DeleteStudent(con,id);
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

    static int Studentcnt(Connection con,int tid)
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

    static int AddStudent(Connection con,int tid,String[] l)
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

    static int UpdateStudent(Connection con,int tid,String[] l)
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

    static int DeleteStudent(Connection con,int tid)
    {
        try{
            Statement del_stmt = con.createStatement();
            del_stmt.execute("DELETE FROM STUDENTS;");
            con.commit();

            return 1;
        }catch(SQLException e){
            System.out.println("DeleteStudent error!!");
            e.printStackTrace();
            return -1;
        }
    }

    public static void summarize()
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
                else
                {
                    fout.write("DeleteStudent");
                }

                fout.write(") \n");  
            }

            fout.write("\n");
        }catch(IOException e){
            System.out.println("Summary Error");
        }
    }
}