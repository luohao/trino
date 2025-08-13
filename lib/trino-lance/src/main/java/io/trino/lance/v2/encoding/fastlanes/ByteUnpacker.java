/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.lance.v2.encoding.fastlanes;

public class ByteUnpacker
{
    public static final int R = 1024;
    public static final int T = 8;
    public static final int LANES = R / T;
    public static final int[] FL_ORDER = new int[] {0, 4, 2, 6, 1, 5, 3, 7};

    private ByteUnpacker() {}

    public static int index(int row, int lane)
    {
        int o = row / 8;
        int s = row % 8;
        return FL_ORDER[o] * 16 + s * 128 + lane;
    }

    public static byte mask(int width)
    {
        if (width == T) {
            return (byte) 0xFF;
        }
        else {
            return (byte) ((1 << (width % T)) - 1);
        }
    }

    public static void unpack(byte[] input, int width, byte[] output)
    {
        for (int lane = 0; lane < LANES; lane++) {
            byte src = input[lane];
            byte tmp;

            for (int row = 0; row < T; row++) {
                int curr = (row * width) / T;
                int next = ((row + 1) * width) / T;

                int shift = (row * width) % T;
                if (next > curr) {
                    int remainingBits = ((row + 1) * width) % T;
                    int currentBits = width - remainingBits;
                    tmp = (byte) ((src >> shift) & mask(currentBits));
                    if (next < width) {
                        src = input[LANES * next + lane];
                        tmp |= (src & mask(remainingBits)) << currentBits;
                    }
                }
                else {
                    tmp = (byte) ((src >> shift) & mask(width));
                }

                int idx = index(row, lane);
                output[idx] = tmp;
            }
        }
    }
}
