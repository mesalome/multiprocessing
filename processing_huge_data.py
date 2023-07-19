import multiprocessing as mp
import os
from timeit import timeit

#each line is string and we modify them into int
def process_line(line):
    return int(line.strip())  

def process_chunk(file_name, chunk_start, chunk_end):
    chunk_sum = 0
    chunk_count = 0  # To count data points in this chunk
    with open(file_name, 'r') as f:
        # Move position to `chunk_start`
        f.seek(chunk_start)

        # Read and process lines until `chunk_end`
        for line in f:
            chunk_start += len(line)
            if chunk_start > chunk_end:
                break
            chunk_sum += process_line(line)
            chunk_count += 1
    return chunk_sum, chunk_count

def parallel_read_and_sum(file_name):
    cpu_count = mp.cpu_count()
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    chunk_args = []
    with open(file_name, 'r') as f:
        def is_start_of_line(position):
            if position == 0:
                return True
            # Check whether the previous character is end of line
            f.seek(position - 1)
            return f.read(1) == '\n'

        def get_next_line_position(position):
            # Read the current line till the end
            f.seek(position)
            f.readline()
            # Return a position after reading the line
            return f.tell()

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            # Make sure the chunk ends at the beginning of the next line
            while not is_start_of_line(chunk_end):
                chunk_end -= 1

            # Handle the case when a line is too long to fit the chunk size
            if chunk_start == chunk_end:
                chunk_end = get_next_line_position(chunk_end)

            args = (file_name, chunk_start, chunk_end)
            chunk_args.append(args)

            # Move to the next chunk
            chunk_start = chunk_end

    with mp.Pool(cpu_count) as p:
        # Run chunks in parallel and get the sum and count of each chunk
        chunk_sums_and_counts = p.starmap(process_chunk, chunk_args)

    total_sum = sum(chunk_sum for chunk_sum, _ in chunk_sums_and_counts)
    total_count = sum(chunk_count for _, chunk_count in chunk_sums_and_counts)
    return total_sum, total_count

if __name__ == '__main__':
    file_name = 'big_data.txt'

    # Measure the execution time of the parallel_read_and_sum function
    time_taken = timeit(lambda: parallel_read_and_sum(file_name), number=1)

    total_sum, total_count = parallel_read_and_sum(file_name)
    average = total_sum / total_count
    print('Total Sum:', total_sum)
    print('Average:', average)
    print('Time taken:', time_taken, 'seconds')
