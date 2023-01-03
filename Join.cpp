#include "Join.hpp"
#include <functional>
/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(
    Disk* disk, 
    Mem* mem, 
    pair<unsigned int, unsigned int> left_rel, 
    pair<unsigned int, unsigned int> right_rel) 
{
    vector<Bucket> buckets;
    for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        Bucket b(disk);        
        buckets.push_back(b);
    }

    Page* input_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
    unsigned int partition_num = 0;
    unsigned int page_written = 0;
    unsigned int loop_start = left_rel.first;
    unsigned int loop_end = left_rel.second;
    bool right = 0;
    

    for (int k = 0; k < 2; k++) {       // runs onces for left partition, once for right
        if (right) {                    // false on first loop
            loop_start = right_rel.first;
            loop_end = right_rel.second;
        }
        mem->reset();
        for (unsigned int i = loop_start; i < loop_end; i++) {              // Partitioning LEFT/RIGHT relation
            mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);                                  // load the next page of relation into input_buffer     
            for (unsigned int j = 0; j < input_buffer->size(); j++) {       // iterate records in input_buffer
                partition_num = input_buffer->get_record(j).partition_hash() % (MEM_SIZE_IN_PAGE - 1);  // get this record, hash it, get partition # (1,2,..,(B-1))
                mem->mem_page(partition_num)->loadRecord(input_buffer->get_record(j));  // add this record to the mem buffer it hashed to
                if (mem->mem_page(partition_num)->full()) {                 // check if that load filled the buffer page
                    page_written = mem->flushToDisk(disk, partition_num);
                    if (!right) {       // partitioning left_rel 
                        buckets[partition_num].add_left_rel_page(page_written); // add the full page just written to memory to this partition bucket
                    }
                    else {
                        buckets[partition_num].add_right_rel_page(page_written);
                    }
                }// output flushed or wasn't full, go to next record in input_buffer                
            } // exhausted input_buffer, get new page
        } // end partitioning of this relation
        for (unsigned int n = 0; n < MEM_SIZE_IN_PAGE - 1; n++) {           // flush all memory buffers to disk and update bucket vector before partitioning next relation
            if (mem->mem_page(n)->size() > 0) {                             // writing mem buffers that contain records
                page_written = mem->flushToDisk(disk, n);
                if (!right) {
                    buckets[n].add_left_rel_page(page_written);
                }
                else {
                    buckets[n].add_right_rel_page(page_written);
                }                
            }   
        }        
        right = 1;
    }      
    return buckets;    
}
/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {     
    Page* input_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
    Page* output_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 2);
    unsigned int input_page_id = MEM_SIZE_IN_PAGE - 1, unsigned int output_page_id = MEM_SIZE_IN_PAGE - 2;
    unsigned int hash_bucket = 0, page_written = 0, left_rel_page_num = 0, right_rel_page_num = 0, smaller_rel = 1; //0-left_rel is smaller. 
    vector<unsigned int> join_res, smaller_rel_vec, larger_rel_vec;                                                 //1-right_rel is smaller or left==right

    for (unsigned int i = 0; i < partitions.size(); i++) {          // Find smaller relation
        left_rel_page_num += partitions[i].get_left_rel().size();
        right_rel_page_num += partitions[i].get_right_rel().size();
    }
    if (left_rel_page_num < right_rel_page_num) {                   // else right_rel is smaller & smaller_rel = 1
        smaller_rel = 0;
    }
    // Build and probe for each partition
    for (unsigned int i = 0; i < partitions.size(); i++) {          // input mem_page was never pushed as a bucket, read all 
        if (smaller_rel == 0) {                                     // start of next partition, build hashtable
            smaller_rel_vec = partitions[i].get_left_rel();
            larger_rel_vec = partitions[i].get_right_rel();
        }
        else {
            smaller_rel_vec = partitions[i].get_right_rel();
            larger_rel_vec = partitions[i].get_left_rel();
        }                
        for (unsigned int b = 0; b < MEM_SIZE_IN_PAGE - 2; b++) {   // reset B-2 buffer pages (hash table) for next hashtable
            mem->mem_page(b)->reset();
        }
        for (unsigned int j = 0; j < smaller_rel_vec.size(); j++) { // iterate this bucket's vector of page_ids and build hashtable from smaller_rel            
            mem->loadFromDisk(disk, smaller_rel_vec[j], input_page_id);                   // load records in disk[page_id] to input_buffer            
            for (unsigned int k = 0; k < input_buffer->size(); k++) {                     // iterate records in input_buffer
                hash_bucket = input_buffer->get_record(k).probe_hash() % (MEM_SIZE_IN_PAGE - 2); // calculate hash of record k
                mem->mem_page(hash_bucket)->loadRecord(input_buffer->get_record(k));      // load each record into corresponding bucket in hashtable                
            } // hashed entire input_buffer
        }// repeat until exhausted all page_id's for smaller_rel in partition i
        // finished building in-memory hashtable for partition i
        // Probe against hashtable for partition i 
        for (unsigned int j = 0; j < larger_rel_vec.size(); j++) {        // iterate this bucket's vector of pages for larger_rel            
            mem->loadFromDisk(disk, larger_rel_vec[j], input_page_id);    // load probing disk page's records into input_buffer, buffer cleared before load
            for (unsigned int k = 0; k < input_buffer->size(); k++) {                     // iterate records in input_buffer                
                hash_bucket = input_buffer->get_record(k).probe_hash() % (MEM_SIZE_IN_PAGE - 2);    // calculate record k's hash
                for (unsigned int n = 0; n < mem->mem_page(hash_bucket)->size(); n++) {   // iterate records in hashed-to bucket                    
                    if (input_buffer->get_record(k) == mem->mem_page(hash_bucket)->get_record(n)) { // check if input_record == hash_table_record
                        output_buffer->loadPair(input_buffer->get_record(k), mem->mem_page(hash_bucket)->get_record(n));    // loadPair to output_buffer
                        if (output_buffer->full()) {
                            page_written = mem->flushToDisk(disk, output_page_id);        // flush output to disk, push page_id written-to to result vector 
                            join_res.push_back(page_written);
                        }
                    }                                                                    
                }                    
            } // done with records in input_buffer, read next probe page_id
        } // done with partition's probing and hashtable
        // if !empty, flush output buffer before repeating for next partition        
    } // outer for
    if (output_buffer->size() > 0) {   
        page_written = mem->flushToDisk(disk, output_page_id); 
        join_res.push_back(page_written);
    }  // only flush a partially-full buffer at the end of 
       // all partitions
    return join_res;
}