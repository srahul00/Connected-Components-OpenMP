#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>

void disp(int **a, int n)
{
    printf("Array Values\n");
    for(int i = 0; i<n; i++)
        printf("%d %d %d\n", a[i][0], a[i][1], a[i][2]);
}

void VerticesEdges(int vertices[], int* verticesCount, int* edgesCount)
{
    FILE *f = fopen("testfile1.txt", "r");
    int i = 0;
    int edge = 0;
    int x = 0, y = 0;
    while(fscanf(f, "%d%d", &x, &y) != -1)
    {
        int flagx = 1, flagy = 1;
        for(int j = i-1; j>=0; j--)
        {
            if(vertices[j] == x)
                flagx = 0;
            if(vertices[j] == y)
                flagy = 0;
        }
        if(flagx)
            vertices[i++] = x;
        if(flagy && x != y)
            vertices[i++] = y;
        edge++;
    }
    fclose(f);
    *verticesCount = i;
    *edgesCount = edge;
}

void AppendVertices(int **a, int* index)
{
    int vertices[10000] = {0}, temp = 0;
    VerticesEdges(vertices, index, &temp);
    
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<*index; i++)
    {
        #pragma omp critical
        {
            a[i][0] = vertices[i]; 
            a[i][2] = vertices[i];
            a[i][1] = -1;
            a[i][3] = 0;
        }
    }
}

void input(int **a) 
{
    int i = 0;
    AppendVertices(a, &i);
    int z = i;
    FILE *f = fopen("testfile1.txt", "r");
    int x, y;
    while(fscanf(f, "%d%d", &x, &y) != -1)
    {
        a[i][0] = x;
        a[i][1] = -1; 
        a[i][2] = y;
        a[i][3] = 0;
        a[i+1][2] = x;
        a[i+1][1] = -1;
        a[i+1][0] = y;
        a[i+1][3] = 0;
        i += 2;
    }
    fclose(f);
}

 
void merge(int **a, int i1, int j1, int i2, int j2, int comparator)
{
    int temp[10000][3];    
    int i = i1;    
    int j = i2;    
    int k = 0;
    
    while(i<=j1 && j<=j2)   
    {
        if(a[i][comparator] < a[j][comparator])
        {
            temp[k][2-comparator] = a[i][2-comparator];
            temp[k][comparator] = a[i][comparator];
            temp[k++][1] = a[i++][1];
        }
        
        else
        {
            temp[k][2-comparator] = a[j][2-comparator];
            temp[k][comparator] = a[j][comparator];
            temp[k++][1] = a[j++][1];
        }
    }
    
    while(i<=j1)  
    {
            temp[k][2-comparator] = a[i][2-comparator];
            temp[k][comparator] = a[i][comparator];
            temp[k++][1] = a[i++][1];
    }
        
    while(j<=j2) 
    {
            temp[k][2-comparator] = a[j][2-comparator];
            temp[k][comparator] = a[j][comparator];
            temp[k++][1] = a[j++][1];
    }
        
    for(i = i1,j = 0; i <= j2; i++, j++)
    {
            a[i][2-comparator] = temp[j][2-comparator];
            a[i][comparator] = temp[j][comparator];
            a[i][1] = temp[j][1];
    }
}

void mergesort(int **a, int i, int j, int comparator)
{
    int mid;
        
    if(i<j)
    {
        mid=(i+j)/2;
        #pragma omp parallel sections 
        {

            #pragma omp section
            {
                mergesort(a, i, mid, comparator);      
            }
            #pragma omp section
            {
                mergesort(a, mid+1, j, comparator);   
            }
        }
        merge(a, i, mid, mid+1, j, comparator);
    }
}

void copy2darray(int **var, int **a, int n)
{
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<n; i++)
    {
        #pragma omp critical
        {
            var[i][0] = a[i][0];
            var[i][1] = a[i][1];
            var[i][2] = a[i][2];
            var[i][3] = a[i][3];
        }
    }
}

int minimump(int **m, int n, int u)
{
    int mini = -1;
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<n; i++)
    {
        #pragma omp critical
        {
            if(m[i][2] == u && m[i][0] < mini)
                mini = m[i][0];
            else if(m[i][2] == u && mini == -1)
                mini = m[i][0];
        }
        
    }
    return mini;
}

int minimumq(int **c, int n, int p)
{
    int mini = -1;
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<n; i++)
    {
        if(c[i][1] == -1)
            continue;
        #pragma omp critical
        {
            if(c[i][0] == p && c[i][1] < mini )
                mini = c[i][1];
            else if(c[i][0] == p && mini == -1)
                mini = c[i][1];
        }
    }
    return mini;
}

void Partitions(int partitions[], int *partitionCount, int **a, int n)
{
    int partcount = 0;
    for(int i = 0; i<n; i++)
    {
        
        int temp = a[i][0];
        int flag = 1;
        for(int j = i-1; j>=0; j--)
        {
            if(partitions[j] == temp)
                flag = 0;
        }
        if(flag)
        {
            partitions[i] = temp;
            partcount++;
        }
    }
    *partitionCount = partcount;
}

int** append(int **a, int n, int pmin)
{
    a = (int**)realloc(a, (n+1)*sizeof(int*));
    a[n] = (int*)malloc(4*sizeof(int));
    a[n][0] = pmin;
    a[n][1] = -1;
    a[n][2] = pmin;
    a[n][3] = 1;
    return a;
}

int** erase(int **a, int nNew, int n)
{
    int **t;
    t = malloc(nNew * sizeof(int*));
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<nNew; i++)
    {
        #pragma omp critical
        t[i] = (int*)malloc(4*sizeof(int));
    }
    
    int i = 0, j = 0;
    for(i = 0; i<nNew; i++)
    {
        if(a[i][3] != 1) // Not temp tuple
        {
            memmove(t[i], a[j], 4* sizeof(int));
            j++;
        }
    }
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<n; i++)
    {
        #pragma omp critical
        free(a[i]);
    }
    free(a);
    
    a = (int**)malloc(nNew*sizeof(int*));
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<nNew; i++)
    {
        #pragma omp critical
        a[i] = t[i];
    }
    free(t);
    return a;
}

int** ParallelSV(int **a, int *num)
{
    int n = *num;
    int loopi = 1;
    int **m, **c; 
    bool converged = false;
    while(converged != true)
    {
        converged = true;
        
        m = (int**)malloc(n * sizeof(int*));
        #pragma omp parallel for schedule(auto)
        for(int i = 0; i<n; i++)
        {
            #pragma omp critical(m)
            m[i] = (int*)malloc(4*sizeof(int));
        }
        copy2darray(m, a, n);
        mergesort(m, 0, n-1, 2);
        #pragma omp parallel for schedule(auto)
        for(int u = 0; u<n; u++)
        {
            int umin = minimump(m, n, u);
            for(int i = 0; i<n; i++)
            {
                #pragma omp critical
                if(a[i][2] == u)
                    a[i][1] = umin;
                
            }
        }
        #pragma omp parallel for schedule(auto)
        for(int i = 0; i<n; i++)
        {   
            #pragma omp critical
            free(m[i]);
        }
        free(m);            

        c = (int**)malloc(n * sizeof(int*));
        #pragma omp parallel for schedule(auto)
        for(int i = 0; i<n; i++)
        {
            #pragma omp critical
            c[i] = (int*)malloc(4*sizeof(int));
        }
        copy2darray(c, a, n);
        mergesort(c, 0, n-1, 0);
        int partitions[10000] = {0}, partitionCount = 0;
        Partitions(partitions, &partitionCount, a, n);
        int appendcount = 0;
        int orgn = n;
        for(int ploop = 0; ploop<partitionCount; ploop++)
        {
            int p = partitions[ploop];
            int pmin = minimumq(c, orgn, p);
            if(p != pmin)
                converged = false;
            for(int i = 0; i<n; i++)
            {
                if(a[i][0] == p)
                    a[i][0] = pmin;
            }
            a = append(a, n, pmin);// temp tuples
            appendcount += 1;
            n += 1;
        }
        for(int i = 0; i<orgn; i++)
            free(c[i]);
        free(c);


        //***Second Time to enable Pointer Jumping***
        m = (int**)malloc(n * sizeof(int*));
        #pragma omp parallel for schedule(auto)
        for(int i = 0; i<n; i++)
        {
            #pragma omp critical(m)
            m[i] = (int*)malloc(4*sizeof(int));
        }
        copy2darray(m, a, n);
        mergesort(m, 0, n-1, 2);
        #pragma omp parallel for schedule(auto)
        for(int u = 0; u<n; u++)
        {
            int umin = minimump(m, n, u);
            for(int i = 0; i<n; i++)
            {
                #pragma omp critical
                if(a[i][2] == u)
                    a[i][1] = umin;
            }
        }
        #pragma omp parallel for schedule(auto)
        for(int i = 0; i<n; i++)
        {   
            #pragma omp critical
            free(m[i]);
        }
        free(m);          

        c = (int**)malloc(n * sizeof(int*));
        #pragma omp parallel for schedule(auto)
        for(int i = 0; i<n; i++)
        {
            #pragma omp critical
            c[i] = (int*)malloc(4*sizeof(int));
        }
        copy2darray(c, a, n);
        mergesort(c, 0, n-1, 0);
        partitionCount = 0;
        Partitions(partitions, &partitionCount, a, n);
        orgn = n;
        for(int ploop = 0; ploop<partitionCount; ploop++)
        {
            int p = partitions[ploop];
            int pmin = minimumq(c, orgn, p);
            if(p != pmin)
                converged = false;
            for(int i = 0; i<n; i++)
            {
                if(a[i][0] == p)
                    a[i][0] = pmin;
            }
            a = append(a, n, pmin);
            appendcount += 1;
            n += 1;
        }
        #pragma omp parallel for schedule(auto)
        for(int i = 0; i<orgn; i++)
        {
            #pragma omp critical
            free(c[i]);
        }
            
        free(c);

        a = erase(a, n-appendcount, n);
        n = n - appendcount;
        //printf("Loop Number : %d\n", loopi);
        loopi += 1;
    }
    *num = n;
    return a;
}

void ConnectedComponentsDisp(int **a, int verticesCount)
{
    printf("The Connected Components are : \n");
    mergesort(a, 0, verticesCount-1, 0);
    int prev = a[0][0];
    for(int i = 0; i<verticesCount; i++)
    {
        if(prev != a[i][0])
        {
            printf("\n");
            prev = a[i][0];
        }
        printf("%d ", a[i][2]);
    }
}

int main() 
{
    int n, verticesCount = 0, edgesCount = 0, vertices[10000];
    VerticesEdges(vertices, &verticesCount, &edgesCount);
    n = verticesCount + 2*edgesCount;
    //printf("n : %d\n", n);
    int **a;
    a = (int **)malloc(n * sizeof(int*));
    #pragma omp parallel for schedule(auto)
    for(int i = 0; i<n; i++)
    {
        #pragma omp critical
        a[i] = (int*)malloc(4*sizeof(int));
    }
    input(a);
    a = ParallelSV(a, &n);
    //disp(a, n);
    ConnectedComponentsDisp(a, verticesCount);
    return 0;
}