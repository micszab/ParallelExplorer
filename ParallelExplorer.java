import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.locks.LockSupport;

public class ParallelExplorer68 implements Explorer {
    private ThreadsFactory threadsFactory;
    private Table2D table;

    private final ConcurrentHashMap<Position2D, Integer> sharedValues = new ConcurrentHashMap<>();
    private final Set<Pair> detectedPairs = ConcurrentHashMap.newKeySet();
    private final BlockingQueue<int[]> taskQueue = new LinkedBlockingQueue<>();
    private final ConcurrentLinkedQueue<Position2D> positionsToZero = new ConcurrentLinkedQueue<>();
    private volatile boolean isStarted = false;
    private final AtomicBoolean isFinished = new AtomicBoolean(false);
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicBoolean hasNewZeroPosition = new AtomicBoolean(false);
    private List<ThreadAndPosition> readerThreads;
    private Thread writerThread;

    private final ThreadLocal<Thread> writerThreadLocal = ThreadLocal.withInitial(() -> null);

    @Override
    public void setThreadsFactory(ThreadsFactory factory) {
        if (!isStarted) {
            this.threadsFactory = factory;
        } else {
            throw new IllegalStateException("Cannot change ThreadsFactory after the process has started.");
        }
    }

    @Override
    public void setTable(Table2D table) {
        if (!isStarted) {
            this.table = table;
        } else {
            throw new IllegalStateException("Cannot change Table2D after the process has started.");
        }
    }

    @Override
    public void start(int targetSum) {
        isStarted = true;
        validateState();

        int rows = table.rows();
        int cols = table.cols();
        allocateTasks(rows, cols);

        writerThread = threadsFactory.writterThread(this::zeroPositions);
        writerThreadLocal.set(writerThread);
        writerThread.start();

        readerThreads = startReaderThreads(targetSum);

        new Thread(() -> waitForCompletion(readerThreads, writerThread)).start();
    }

    @Override
    public Set<Pair> result() {
        if (!isFinished.get()) {
            return Collections.emptySet();
        }
        return new HashSet<>(detectedPairs);
    }

    private void validateState() {
        if (threadsFactory == null || table == null) {
            throw new IllegalStateException("ThreadsFactory or Table2D is not set.");
        }
    }

    private void allocateTasks(int rows, int cols) {
        int numThreads = threadsFactory.readersThreads();
        int blockSize = Math.max(1, (int) Math.sqrt((rows * cols) / numThreads));
        int overlap = 1;

        for (int row = 0; row < rows; row += blockSize - overlap) {
            for (int col = 0; col < cols; col += blockSize - overlap) {
                int[] task = {
                        row, Math.min(row + blockSize, rows),
                        col, Math.min(col + blockSize, cols)
                };
                taskQueue.add(task);
            }
        }
    }

    private List<ThreadAndPosition> startReaderThreads(int targetSum) {
        List<ThreadAndPosition> readers = new ArrayList<>();
        for (int i = 0; i < threadsFactory.readersThreads(); i++) {
            ThreadAndPosition reader = threadsFactory.readerThread(() -> executeTasks(targetSum));
            if (reader != null) {
                readers.add(reader);
                reader.thread().start();
            }
        }
        return readers;
    }

    private void executeTasks(int targetSum) {
        activeThreads.incrementAndGet();
        try {
            int[] task;
            while ((task = taskQueue.poll(100, TimeUnit.MILLISECONDS)) != null) {
                searchForPairs(
                        task[0], task[1], task[2], task[3], targetSum,
                        new HashSet<Position2D>()
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (activeThreads.decrementAndGet() == 0 && taskQueue.isEmpty()) {
                isFinished.set(true);
                Thread writer = writerThreadLocal.get();
                if (writer != null) {
                    LockSupport.unpark(writer);
                }
            }
        }
    }

    private void searchForPairs(int rowStart, int rowEnd, int colStart, int colEnd, int targetSum, Set<Position2D> visitedPositions) {
        int[][] directions = {{0, 1}, {1, 0}, {1, 1}, {-1, 1}};
        Map<Position2D, Integer> localValues = new HashMap<>();

        for (int row = rowStart; row < rowEnd; row++) {
            for (int col = colStart; col < colEnd; col++) {
                Position2D currentPos = new Position2D(col, row);
                if (visitedPositions.contains(currentPos)) {
                    continue;
                }

                int currentValue;
                if (localValues.containsKey(currentPos)) {
                    currentValue = localValues.get(currentPos);
                } else {
                    if (sharedValues.containsKey(currentPos)) {
                        currentValue = sharedValues.get(currentPos);
                    } else {
                        currentValue = table.get(currentPos);
                        sharedValues.put(currentPos, currentValue);
                    }
                    localValues.put(currentPos, currentValue);
                }

                visitedPositions.add(currentPos);

                for (int[] dir : directions) {
                    int newRow = row + dir[0];
                    int newCol = col + dir[1];
                    if (newRow >= rowStart && newRow < rowEnd && newCol >= colStart && newCol < colEnd) {
                        Position2D neighborPos = new Position2D(newCol, newRow);

                        int neighborValue;
                        if (localValues.containsKey(neighborPos)) {
                            neighborValue = localValues.get(neighborPos);
                        } else {
                            if (sharedValues.containsKey(neighborPos)) {
                                neighborValue = sharedValues.get(neighborPos);
                            } else {
                                neighborValue = table.get(neighborPos);
                                sharedValues.put(neighborPos, neighborValue);
                            }
                            localValues.put(neighborPos, neighborValue);
                        }

                        if (currentValue + neighborValue == targetSum) {
                            addPairAndMarkForZero(currentPos, neighborPos);
                        }
                    }
                }
            }
        }
        if (visitedPositions.size() == table.rows() * table.cols()) {
            isFinished.set(true);
            return;
        }
    }

    private void addPairAndMarkForZero(Position2D pos1, Position2D pos2) {
        Pair pair = new Pair(pos1, pos2);
        if (detectedPairs.add(pair)) {
            positionsToZero.offer(pos1);
            positionsToZero.offer(pos2);
            hasNewZeroPosition.set(true);
            Thread writer = writerThreadLocal.get();
            if (writer != null) {
                LockSupport.unpark(writer);
            }
        }
    }

    private void zeroPositions() {
        while (true) {
            Position2D pos = positionsToZero.poll();
            if (pos != null) {
                table.set0(pos);
            } else {
                if (isFinished.get() && positionsToZero.isEmpty() && activeThreads.get() == 0) {
                    break;
                }
                hasNewZeroPosition.set(false);
                LockSupport.park();
            }
        }
    }

    private void waitForCompletion(List<ThreadAndPosition> readers, Thread writerThread) {
        readers.forEach(r -> {
            try {
                r.thread().join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        isFinished.set(true);
        LockSupport.unpark(writerThread);

        try {
            writerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
