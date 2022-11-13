//
//  main.cpp
//  test
//
//  Created by MSD2 on 11/12/22.
//

#include <iostream>
#include <algorithm>
int dd[3][5] = {
    {1, 2, 3, 8, 9},
    {11, 12, 13, 14, 15},
    {4, 5, 6, 7, 10}
};

// model
// guess( 1st row/iterator, 7) -> ? use this to build a distinct ranges

using namespace std;

struct Iterator {
    int *p;
    int lim;
    int cur;
    
    Iterator(int *p, int lim) {
        this->p = p;
        this->lim = lim;
        this->cur = 0;
    }
    
    bool valid() {
        return (cur < lim);
    }
    
    int peek() { // key
        return p[cur];
    }
    
    void consume() { // next
        cur++;
    }
    
    int get_pos() {
        return cur;
    }
    
    Iterator *getSubRangeIterator(int start, int len) {
        return new Iterator(p + start, len);
    }

    int guess(int item) {
        for(int i=0; i<lim; i++){
            if(item < p[i]){
                return i;
            }  
        }
        return lim;
    }

    int get_lim() {
        return lim;
    }
};

struct MergingIterator {
    Iterator *children[3];
    Iterator *current;
    
    MergingIterator(Iterator *p1, Iterator *p2, Iterator *p3) {
        children[0] = p1;
        children[1] = p2;
        children[2] = p3;
        current = findSmallest();
    }
    
    bool valid() {
        if ( children[0]->valid() || children[1]->valid() || children[2]->valid()) return true;
        return false;
    }
    
    int key() {
        return current->peek();
    }
    
    void next() {
        current->consume(); // consume it.
        current = findSmallest();
    }
    
    Iterator* findSmallest() {
        int n = 3;
        Iterator *smallest = nullptr;
        for (int i=0; i < n; i++) {
            if (!children[i]->valid()) continue;
            if (smallest == nullptr) {
                smallest = children[i];
            }
            if (smallest->peek() > children[i]->peek()) {
                smallest = children[i];
            }
        }
        return smallest;
    }
};

struct DistinctRangeIterator {
    Iterator *children[3];
    Iterator *current;
    
    DistinctRangeIterator(Iterator *p1, Iterator *p2, Iterator *p3) {
        children[0] = p1;
        children[1] = p2;
        children[2] = p3;
    }
    
    Iterator *getNextRangeIterator() {
        current = findSmallest();
        int start = current->get_pos();
       // int cnt = 1;
       int minPosition = current->get_lim();
        for(int i=0;i<3;i++){
            if(current == children[i]){continue;}
            if(children[i]->valid()){
                int guessPos = current->guess(children[i]->peek());
                minPosition = min(minPosition, guessPos);
            }
        }
        int end = minPosition;
        int cnt = end - start;
        int i=start;
        while(i<end){
            current->consume();
            i++;
        }
        cout<<"Range: "<<start<<" "<<cnt<<endl;
        return current->getSubRangeIterator(start, cnt);
        /* BACKUP
        while (current->valid()) {
            current->consume();
            Iterator *next = findSmallest();
            if (current == next) {
                cnt++;
            } else {
                break;
            }
        }
        */
        
       //cout<<"Range: "<<start<<" "<<cnt<<endl;
//return current->getSubRangeIterator(start, cnt);
    }
    
    Iterator* findSmallest() {
        int n = 3;
        Iterator *smallest = nullptr;
        for (int i=0; i < n; i++) {
            if (!children[i]->valid()) continue;
            if (smallest == nullptr) {
                smallest = children[i];
            }
            if (smallest->peek() > children[i]->peek()) {
                smallest = children[i];
            }
        }
        return smallest;
    }
    
    bool valid() {
        if ( children[0]->valid() || children[1]->valid() || children[2]->valid()) return true;
        return false;
    }


    
};



int main(int argc, const char * argv[]) {
    Iterator i1(dd[0], 5);
    Iterator i2(dd[1], 5);
    Iterator i3(dd[2], 5);

    /*
    MergingIterator m(&i1, &i2, &i3);
    
    
    while (m.valid()) {
        std::cout<<m.key()<<std::endl;
        m.next();
    }
     */
    
    DistinctRangeIterator dri(&i1, &i2, &i3);
    while (dri.valid()) {
        Iterator *current = dri.getNextRangeIterator();
        while(current->valid()) {
            cout<<current->peek()<<" ";
            current->consume();
        }
        cout<<endl;
    }
    return 0;
}

