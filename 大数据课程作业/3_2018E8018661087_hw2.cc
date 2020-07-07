/* 3, 2018E8018661087, Li Yun */



#include <math.h>

#include <stdio.h>

#include <string.h>

#include <iostream>

#include <vector>

#include "GraphLite.h"

using namespace std;


/*the value of vertex*/
typedef struct VertexStruct

{

    int64_t vertexID;

    vector<unsigned long long> inNeigh;

    //int InTriangle;

    //int OutTriangle;

    int ThroughTriangle;

    int CycleTriangle;

} VertexStruct;





#define VERTEX_CLASS_NAME(name) DirectedTriangleCount##name


class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {

public:

    int64_t getVertexNum() {

        unsigned long long n;

        sscanf(m_ptotal_vertex_line, "%lld", &n);

        m_total_vertex = n;

        return m_total_vertex;

    }

    int64_t getEdgeNum() {

        unsigned long long n;

        sscanf(m_ptotal_edge_line, "%lld", &n);

        m_total_edge = n;

        return m_total_edge;

    }

    int getVertexValueSize() {

        m_n_value_size = sizeof(VertexStruct);

        return m_n_value_size;

    }

    int getEdgeValueSize() {

        m_e_value_size = sizeof(int);

        return m_e_value_size;

    }

    int getMessageValueSize() {

        m_m_value_size = sizeof(int64_t);

        return m_m_value_size;

    }

    void loadGraph() {

        unsigned long long last_vertex;

        unsigned long long from;

        unsigned long long to;

        double weight = 0;



        VertexStruct value;

        //value.InTriangle = 0;

        //value.OutTriangle = 0;

        value.ThroughTriangle = 0;

        value.CycleTriangle = 0;



        int outdegree = 0;



        const char *line= getEdgeLine();



        // Note: modify this if an edge weight is to be read

        //       modify the 'weight' variable


        sscanf(line, "%lld %lld", &from, &to);

        addEdge(from, to, &weight);



        last_vertex = from;

        ++outdegree;



        for (int64_t i = 1; i < m_total_edge; ++i) {

            line= getEdgeLine();



            // Note: modify this if an edge weight is to be read

            //       modify the 'weight' variable



            sscanf(line, "%lld %lld", &from, &to);

    

            if (last_vertex != from) {

                value.vertexID = last_vertex;

                addVertex(last_vertex, &value, outdegree);

                last_vertex = from;

                outdegree = 1;

                memset(&value, 0, sizeof(VertexStruct));


            } else {

                ++outdegree;


            }

            addEdge(from, to, &weight);

        }

        value.vertexID = last_vertex;

        addVertex(last_vertex, &value, outdegree);

    }

};


int through = 0;
int cycle = 0;


class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {

public:

    void writeResult() {

        int64_t vid;

        VertexStruct value;

        //value.InTriangle = 0;

        //value.OutTriangle = 0;

        value.ThroughTriangle = 0;

        value.CycleTriangle = 0;

        char s[1024] = {0};



        ResultIterator r_iter;

        r_iter.getIdValue(vid, &value);



        int n = sprintf(s, "in: %d\nout: %d\nthrough: %d\ncycle: %d", through, through, through, cycle);


        writeNextResLine(s, n);

    }

};



// An aggregator that records a double value tom compute sum

class VERTEX_CLASS_NAME(Aggregator): public Aggregator<unsigned long long> {

public:

    void init() {

        m_global = 0;

        m_local = 0;

    }

    void* getGlobal() {

        return &m_global;

    }

    void setGlobal(const void* p) {

        m_global = * (int *)p;

    }

    void* getLocal() {

        return &m_local;

    }

    void merge(const void* p) {

        m_global += * (int *)p;

    }

    void accumulate(const void* p) {

        m_local += * (int *)p;

    }

};



class VERTEX_CLASS_NAME(): public Vertex <VertexStruct, int, int64_t> {

public:


    void compute(MessageIterator* pmsgs) {



        VertexStruct vertexA;

        //vertexA.InTriangle = 0;

        //vertexA.OutTriangle = 0;

        vertexA.ThroughTriangle = 0;

        vertexA.CycleTriangle = 0;



        vertexA.vertexID = getVertexId();

        printf("vertexA.vertexID = %lld\n", vertexA.vertexID);



        //int64_t val = getVertexId();

        if (getSuperstep() == 0) {  

            printf("superstep = 0\n"); 

            sendMessageToAllNeighbors(vertexA.vertexID);

            printf("sendMessageToAllNeighbors(myvertexID)\n");

        } 

        else if(getSuperstep() == 1) {

            printf("superstep = 1\n"); 



            for ( ; ! pmsgs->done(); pmsgs->next() ) {

                int64_t inN = pmsgs->getValue();

                vertexA.inNeigh.push_back(inN);

            cout << inN << ' ';

            }

    cout << endl;




        for(OutEdgeIterator iter = getOutEdgeIterator(); !iter.done(); iter.next())

        {

            cout << iter.target() << ' ';

        }

        cout << "note" <<endl;


            *mutableValue() = vertexA;



            for (int i = 0; i < vertexA.inNeigh.size(); i ++)

            {

                sendMessageToAllNeighbors(vertexA.inNeigh[i]);

            }



            voteToHalt();

        }

        else if (getSuperstep() == 2) {

            printf("superstep = 2\n"); 



            for ( ; ! pmsgs->done(); pmsgs->next() ) {

                int64_t inNeighOfInNeigh = pmsgs->getValue();

        cout << inNeighOfInNeigh << endl;



                for (int i = 0; i < getValue().inNeigh.size(); i ++) {

            cout << "in " << inNeighOfInNeigh << ' ' << getValue().inNeigh[i] << endl;

            if (inNeighOfInNeigh == getValue().inNeigh[i])

                    {

                        vertexA.ThroughTriangle ++;

                cout << "vertexA.ThroughTriangle = " << vertexA.ThroughTriangle << endl;

                    }

                }  

                for(OutEdgeIterator iter = getOutEdgeIterator(); !iter.done(); iter.next())

                {

            cout << "out " << inNeighOfInNeigh << ' ' << iter.target() << endl;

                    if (inNeighOfInNeigh == iter.target())

                    {

                        vertexA.CycleTriangle ++;

                cout << "vertexA.CycleTriangle = " << vertexA.CycleTriangle << endl;

                    }

                } 

            }



            //vertexA.InTriangle = vertexA.ThroughTriangle;

            //vertexA.OutTriangle = vertexA.ThroughTriangle;

            

            accumulateAggr(0, &vertexA.ThroughTriangle);

            accumulateAggr(1, &vertexA.CycleTriangle);

            *mutableValue() = vertexA;

            sendMessageToAllNeighbors(vertexA.vertexID);

            voteToHalt();



        }

        else if (getSuperstep() >= 3){

            printf("superstep = 3\n"); 



            vertexA.ThroughTriangle = * (int *)getAggrGlobal(0);

            vertexA.CycleTriangle = * (int *)getAggrGlobal(1);


            //vertexA.InTriangle = vertexA.ThroughTriangle;

            //vertexA.OutTriangle = vertexA.ThroughTriangle;

            through = vertexA.ThroughTriangle;
            cycle = vertexA.CycleTriangle;

            cout << "vertexA.CycleTriangle = " << vertexA.CycleTriangle << endl;

            *mutableValue() = vertexA;

            voteToHalt(); 

            return;

        }

    }

};



class VERTEX_CLASS_NAME(Graph): public Graph {

public:

    VERTEX_CLASS_NAME(Aggregator)* aggregator;



public:

    // argv[0]: PageRankVertex.so

    // argv[1]: <input path>

    // argv[2]: <output path>

    void init(int argc, char* argv[]) {



        setNumHosts(5);

        setHost(0, "localhost", 1411);

        setHost(1, "localhost", 1421);

        setHost(2, "localhost", 1431);

        setHost(3, "localhost", 1441);

        setHost(4, "localhost", 1451);



        if (argc < 3) {

           printf ("Usage: %s <input path> <output path>\n", argv[0]);

           exit(1);

        }



        m_pin_path = argv[1];

        m_pout_path = argv[2];



        aggregator = new VERTEX_CLASS_NAME(Aggregator)[2];

        regNumAggr(2);

        regAggr(0, &aggregator[0]);

        regAggr(1, &aggregator[1]);

    }



    void term() {

        delete[] aggregator;

    }

};



/* STOP: do not change the code below. */

extern "C" Graph* create_graph() {

    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);



    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);

    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);

    pgraph->m_pver_base = new VERTEX_CLASS_NAME();



    return pgraph;

}



extern "C" void destroy_graph(Graph* pobject) {

    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);

    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);

    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);

    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;

}