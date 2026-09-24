(ns kahuna.hash
  "Kahuna's key-space → hash-partition placement rule, reproduced bit for bit.

  Under hash routing a key's partition is a pure function of its key space:
  `HashPlacement.BucketOfKeySpace` in the server, which is Kommander's
  `HashUtils.ConsistentHash` — two xxHash32 digests of the UTF-8 key space
  under fixed seeds, glued into a 64-bit value, then Lamping–Veach jump
  consistent hashing over the partition pool. `/v1/cluster/routing` publishes
  the rule's identifier, pool size and partition offset but not the answer, so
  a harness that wants to know *which* partition a workload writes has to
  compute it.

  Why the harness wants to know: the register workload puts every key in one
  key space, so one partition takes every write and is the only one whose WAL
  ever compacts. A snapshot seed — the thing the placement gate demands — can
  only happen when a learner joins *that* partition. The placement nemesis
  uses this to aim its decommissions at a node hosting it.

  The identifier is checked before the rule is trusted: a server that reports a
  different `hashAlgorithm` gets a nil answer, never a wrong one."
  (:require [clojure.string :as str]))

(def algorithm
  "The identifier `/v1/cluster/routing` reports for the rule implemented here."
  "kahuna.placement-group-jump-xxh32-v1")

(def ^:private mask32 0xFFFFFFFF)

(def ^:private prime1 2654435761)
(def ^:private prime2 2246822519)
(def ^:private prime3 3266489917)
(def ^:private prime4 668265263)
(def ^:private prime5 374761393)

(defn- u32 ^long [^long x] (bit-and x mask32))

(defn- rotl32 ^long [^long x ^long r]
  (u32 (bit-or (bit-shift-left x r)
               (unsigned-bit-shift-right x (- 32 r)))))

(defn- mul32 ^long [^long a ^long b] (u32 (unchecked-multiply a b)))
(defn- add32 ^long [^long a ^long b] (u32 (unchecked-add a b)))

(defn- read-u32
  "Little-endian unsigned 32-bit lane at `i`."
  ^long [^bytes bs ^long i]
  (bit-or (bit-and (aget bs i) 0xFF)
          (bit-shift-left (bit-and (aget bs (+ i 1)) 0xFF) 8)
          (bit-shift-left (bit-and (aget bs (+ i 2)) 0xFF) 16)
          (bit-shift-left (bit-and (aget bs (+ i 3)) 0xFF) 24)))

(defn- round32 ^long [^long acc ^long lane]
  (mul32 (rotl32 (add32 acc (mul32 lane prime2)) 13) prime1))

(defn xxh32
  "xxHash32 of `bs` under `seed`, as an unsigned 32-bit value in a long."
  ^long [^bytes bs ^long seed]
  (let [len     (alength bs)
        seed    (u32 seed)
        stripes (* 16 (quot len 16))
        ;; Four lanes over every whole 16-byte stripe, or the seed alone when
        ;; the input is shorter than one stripe.
        h       (if (>= len 16)
                  (loop [i  0
                         v1 (add32 (add32 seed prime1) prime2)
                         v2 (add32 seed prime2)
                         v3 seed
                         v4 (u32 (unchecked-subtract seed prime1))]
                    (if (< i stripes)
                      (recur (+ i 16)
                             (round32 v1 (read-u32 bs i))
                             (round32 v2 (read-u32 bs (+ i 4)))
                             (round32 v3 (read-u32 bs (+ i 8)))
                             (round32 v4 (read-u32 bs (+ i 12))))
                      (add32 (add32 (rotl32 v1 1) (rotl32 v2 7))
                             (add32 (rotl32 v3 12) (rotl32 v4 18)))))
                  (add32 seed prime5))
        h       (add32 h len)
        ;; Remaining whole 4-byte lanes.
        [i h]   (loop [i stripes h h]
                  (if (<= (+ i 4) len)
                    (recur (+ i 4)
                           (mul32 (rotl32 (add32 h (mul32 (read-u32 bs i) prime3)) 17)
                                  prime4))
                    [i h]))
        ;; Remaining bytes.
        h       (loop [i i h h]
                  (if (< i len)
                    (recur (inc i)
                           (mul32 (rotl32 (add32 h (mul32 (bit-and (aget bs i) 0xFF) prime5)) 11)
                                  prime1))
                    h))
        ;; Avalanche.
        h       (bit-xor h (unsigned-bit-shift-right h 15))
        h       (mul32 h prime2)
        h       (bit-xor h (unsigned-bit-shift-right h 13))
        h       (mul32 h prime3)]
    (bit-xor h (unsigned-bit-shift-right h 16))))

(defn- jump-consistent-hash
  "Lamping–Veach jump consistent hash of a 64-bit `key` (any long; the bits are
  what matter) onto `[0, buckets)`. The arithmetic is the server's: an unsigned
  64-bit LCG step, and the bucket estimate computed in doubles."
  ^long [^long key ^long buckets]
  (loop [key key
         b   -1
         j   0]
    (if (< j buckets)
      (let [key' (unchecked-add (unchecked-multiply key 2862933555777941757) 1)
            j'   (long (* (inc j)
                          (/ 2147483648.0
                             (double (inc (unsigned-bit-shift-right key' 33))))))]
        (recur key' j j'))
      b)))

(def ^:private seed1 0xAAAAAAAA)
(def ^:private seed2 0x55555555)

(defn consistent-hash
  "Kommander's `HashUtils.ConsistentHash`: the bucket in `[0, buckets)` for the
  string `s`."
  ^long [^String s ^long buckets]
  (when-not (pos? buckets)
    (throw (IllegalArgumentException. "buckets must be greater than 0")))
  (let [bs (.getBytes s "UTF-8")
        h1 (xxh32 bs seed1)
        h2 (xxh32 bs seed2)]
    (jump-consistent-hash (bit-or (bit-shift-left h1 32) h2) buckets)))

(defn placement-group
  "The part of a key space that placement hashes: everything before the first
  `|`. Key spaces that share a group land on the same partition by design."
  [key-space]
  (first (str/split key-space #"\|" 2)))

(defn hash-partition
  "The partition a hash-routed key space lands on, given the pool size and the
  first user partition id that `/v1/cluster/routing` reports. Returns nil when
  the reported rule is not the one implemented here — an answer from the wrong
  rule would aim the nemesis at a partition nobody writes."
  [{:keys [hash-algorithm hash-pool-size hash-partition-offset]} key-space]
  (when (and key-space
             (= algorithm hash-algorithm)
             (pos? (or hash-pool-size 0)))
    (+ (or hash-partition-offset 1)
       (consistent-hash (placement-group key-space) hash-pool-size))))
