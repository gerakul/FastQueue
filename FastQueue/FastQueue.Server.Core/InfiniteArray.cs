using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FastQueue.Server.Core
{
    // not thread-safe
    internal class InfiniteArray<T>
    {
        private readonly int blockLength;
        private readonly int dataListCapacity;
        private readonly int minimumFreeBlocks;
        private long offset;
        private List<T[]> data;
        private int firstFreeBlockIndex;
        private int firstBusyBlockIndex;
        private int firstItemIndexInBlock;
        private int firstFreeIndexInBlock;

        internal long NumberOfBlockAllocations { get; private set; }
        internal long NumberDataListAllocations { get; private set; }

        public InfiniteArray(long initialOffset, InfiniteArrayOptions infiniteArrayOptions)
        {
            blockLength = infiniteArrayOptions.BlockLength;
            dataListCapacity = infiniteArrayOptions.DataListCapacity;
            minimumFreeBlocks = infiniteArrayOptions.MinimumFreeBlocks;
            offset = initialOffset;
            data = new List<T[]>(dataListCapacity);
            NumberDataListAllocations = 1;
            data.Add(new T[blockLength]);
            NumberOfBlockAllocations = 1;
            firstFreeBlockIndex = 0;
            firstBusyBlockIndex = 0;
            firstItemIndexInBlock = 0;
            firstFreeIndexInBlock = 0;
        }

        /// <summary>
        /// Add range of items to the array
        /// </summary>
        /// <param name="items">Items</param>
        /// <returns>Index of the new last item in the array</returns>
        public long Add(Span<T> items)
        {
            if (items.Length == 0)
            {
                return GetLastItemIndex();
            }

            if (items.Length <= blockLength - firstFreeIndexInBlock)
            {
                // if block contains enough space just copy the data
                items.CopyTo(data[^1].AsSpan(firstFreeIndexInBlock));
                firstFreeIndexInBlock += items.Length;
            }
            else
            {
                items.Slice(0, blockLength - firstFreeIndexInBlock).CopyTo(data[^1].AsSpan(firstFreeIndexInBlock));
                var sourceInd = blockLength - firstFreeIndexInBlock;
                while (sourceInd + blockLength <= items.Length)
                {
                    StartNewBlock();
                    items.Slice(sourceInd, blockLength).CopyTo(data[^1].AsSpan());
                    sourceInd += blockLength;
                }

                if (sourceInd < items.Length)
                {
                    StartNewBlock();
                    items.Slice(sourceInd).CopyTo(data[^1].AsSpan());
                    firstFreeIndexInBlock = items.Length - sourceInd;
                }
                else
                {
                    firstFreeIndexInBlock = blockLength;
                }

                CheckForCleanUp();
            }

            return GetLastItemIndex();
        }

        /// <summary>
        /// Add one item to the array
        /// </summary>
        /// <param name="item">Item</param>
        /// <returns>Index of the new last item in the array</returns>
        public long Add(T item)
        {
            if (firstFreeIndexInBlock < blockLength)
            {
                data[^1][firstFreeIndexInBlock++] = item;
            }
            else
            {
                StartNewBlock();
                data[^1][0] = item;
                firstFreeIndexInBlock = 1;
                CheckForCleanUp();
            }

            return GetLastItemIndex();
        }

        public void FreeTo(long index)
        {
            if (index < 0)
            {
                throw new IndexOutOfRangeException($"Index must be greater or equal 0. Index: {index}");
            }

            var blockInd = GetBlockIndex(index);
            var indInBlock = GetIndexInBlock(index);

            if (blockInd < firstBusyBlockIndex
                || (blockInd == firstBusyBlockIndex && indInBlock <= firstItemIndexInBlock))
            {
                return;
            }

            if (blockInd > data.Count
                || (blockInd == (data.Count - 1) && indInBlock > firstFreeIndexInBlock)
                || (blockInd == data.Count && indInBlock > 0))
            {
                throw new IndexOutOfRangeException($"Item with index {index} doesn't exist in the array");
            }

            var prevFirstBusyBlockIndex = firstBusyBlockIndex;


            if (blockInd == data.Count && indInBlock == 0)
            {
                firstBusyBlockIndex = blockInd - 1;
                firstItemIndexInBlock = blockLength;
            }
            else
            {
                firstBusyBlockIndex = blockInd;
                firstItemIndexInBlock = indInBlock;
            }

            if (prevFirstBusyBlockIndex != firstBusyBlockIndex)
            {
                CheckForCleanUp();
            }
        }

        public ReadOnlyMemory<T>[] GetDataBlocks()
        {
            var arr = new ReadOnlyMemory<T>[data.Count - firstBusyBlockIndex];

            if (firstBusyBlockIndex == data.Count - 1)
            {
                if (firstItemIndexInBlock == firstFreeIndexInBlock)
                {
                    arr[0] = ReadOnlyMemory<T>.Empty;
                }
                else
                {
                    arr[0] = data[firstBusyBlockIndex].AsMemory(firstItemIndexInBlock, firstFreeIndexInBlock - firstItemIndexInBlock);
                }

                return arr;
            }

            arr[0] = data[firstBusyBlockIndex].AsMemory(firstItemIndexInBlock);
            var ind = 1;
            for (int i = firstBusyBlockIndex + 1; i < data.Count - 1; i++)
            {
                arr[ind++] = data[i].AsMemory();
            }

            arr[ind] = data[^1].AsMemory(0, firstFreeIndexInBlock);

            return arr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StartNewBlock()
        {
            if (firstBusyBlockIndex - firstFreeBlockIndex > 0)
            {
                // if we have free block reuse it
                data.Add(data[firstFreeBlockIndex]);
                data[firstFreeBlockIndex] = null;
                firstFreeBlockIndex++;
            }
            else
            {
                // allocate new block
                data.Add(new T[blockLength]);
                NumberOfBlockAllocations++;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckForCleanUp()
        {
            var busyBlocks = data.Count - firstBusyBlockIndex;
            var freeBlocks = firstBusyBlockIndex - firstFreeBlockIndex;

            if (freeBlocks > (busyBlocks / 2) && freeBlocks > minimumFreeBlocks)
            {
                for (int i = firstFreeBlockIndex; i < firstBusyBlockIndex - minimumFreeBlocks; i++)
                {
                    data[i] = null;
                }

                firstFreeBlockIndex = firstBusyBlockIndex - minimumFreeBlocks;
            }

            var newBlocksTotal = data.Count - firstFreeBlockIndex;
            if (data.Count >= dataListCapacity && newBlocksTotal < (data.Count / 2))
            {
                var newData = new List<T[]>(Math.Max(dataListCapacity, newBlocksTotal));
                NumberDataListAllocations++;
                for (int i = firstFreeBlockIndex; i < data.Count; i++)
                {
                    newData.Add(data[i]);
                }

                data = newData;
                firstBusyBlockIndex -= firstFreeBlockIndex;
                offset += firstFreeBlockIndex * blockLength;
                firstFreeBlockIndex = 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetFirstItemIndex() => offset + firstBusyBlockIndex * blockLength + firstItemIndexInBlock;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLastItemIndex() => offset + (data.Count - 1) * blockLength + firstFreeIndexInBlock - 1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetBlockIndex(long index) => checked((int)((index - offset) / blockLength));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetIndexInBlock(long index) => checked((int)((index - offset) % blockLength));
    }

    public class InfiniteArrayOptions
    {
        public int BlockLength { get; set; } = 100000;
        public int DataListCapacity { get; set; } = 128;
        public int MinimumFreeBlocks { get; set; } = 2;

        public InfiniteArrayOptions()
        {
        }

        public InfiniteArrayOptions(InfiniteArrayOptions options)
        {
            BlockLength = options.BlockLength;
            DataListCapacity = options.DataListCapacity;
            MinimumFreeBlocks = options.MinimumFreeBlocks;
        }
    }
}
