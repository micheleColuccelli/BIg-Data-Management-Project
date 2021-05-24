import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.tx.OTransaction;

import java.util.HashMap;
import java.util.Map;

public class MainGOT {

    public static void main(String[] args) {
        //Open  Database
        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
        //create a session
        //ODatabaseSession class is not thread-safe. When using multiple threads, use separate instances of this class.
        try(ODatabaseSession dbs = orient.open("GamesOfThrones", "admin", "admin");){

            //Query to explain API
            firstQueryGOT(dbs);

            //Query MATCH Syntax
            //secondQueryGOT(dbs);

            //Shortest Path Query
            //thirdQueryGOT(dbs);

            //DFS Query
            //fourthQueryGOT(dbs);

            //close the session
            dbs.close();

            //close the Database
            orient.close();
        }
    }

    private static void firstQueryGOT(ODatabaseSession dbs){

        String query = "SELECT \n" +
                "@rid as Character_RID,\n" +
                "name as Character_Name,\n" +
                "url as Character_Url,\n" +
                "out(\"Has_Family\").size() as FamilySize\n" +
                "FROM Character\n" +
                "ORDER BY FamilySize DESC \n" +
                "LIMIT 10";

        OResultSet rs = dbs.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item.toString());
        }
    }

    private static void secondQueryGOT(ODatabaseSession dbs){

        String query = "MATCH {Class: Character, as: Character, where: (name='Jon Snow')}\n"+
                ".both(){class: Character, as:knownByJon}\n"+
                "-Has_Allegiance->{class: Noble_house, as:noble_house}\n"+
                "RETURN knownByJon.@Rid as knownByJon_RID,knownByJon.name as knownByJon, " +
                "noble_house.name as NobleHouse,noble_house.Sigil as Sigil";

        OResultSet rs = dbs.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }

    private static void thirdQueryGOT(ODatabaseSession dbs){

        String query = "SELECT expand(path) FROM (\n"+
                "SELECT shortestPath($from, $to) AS path\n"+
                "LET\n"+
                "$from = (SELECT FROM Character WHERE name='Jon Snow' ),\n"+
                "$to = (SELECT FROM Religion WHERE name='Valyrian religion' )\n"+
                "UNWIND path\n"+
                ")";

        OResultSet rs = dbs.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.print(item.getProperty("@class").toString()+" -> ");
            System.out.println(item.getProperty("name").toString());
        }
    }

    private static void fourthQueryGOT(ODatabaseSession dbs){

        String query = "TRAVERSE * FROM (\n"+
                "SELECT FROM Character WHERE name = 'Jon Snow'\n"+
                ") MAXDEPTH 2\n";

        OResultSet rs = dbs.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }



}
