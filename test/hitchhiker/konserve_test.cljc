(ns hitchhiker.konserve-test
  (:refer-clojure :exclude [vec])
  (:require [clojure.core.rrb-vector :refer [catvec vec]]
            [#?(:clj clojure.test :cljs cljs.test)
             #?(:clj :refer :cljs :refer-macros) [deftest testing run-tests is use-fixtures
                                                  #?(:cljs async)]]
            [clojure.test.check.clojure-test #?(:clj :refer :cljs :refer-macros) [defspec]]
            [clojure.test.check.generators :as gen #?@(:cljs [:include-macros true])]
            [clojure.test.check.properties :as prop #?@(:cljs [:include-macros true])]
            [konserve.filestore :refer [new-fs-store delete-store list-keys]]
            [konserve.memory :refer [new-mem-store]]
            [hitchhiker.konserve :as kons]
            [konserve.cache :as kc]
            [hasch.core :as hasch]
            [hitchhiker.tree.core #?(:clj :refer :cljs :refer-macros)
             [<?? <? go-try] :as core]
            [hitchhiker.tree.messaging :as msg]
            #?(:clj [hitchhiker.tree.async :refer [case-async]])
            [hitchhiker.ops :refer [recorded-ops]]
            #?(:cljs [cljs.core.async :refer [promise-chan] :as async]
               :clj [clojure.core.async :refer [promise-chan] :as async])
            #?(:cljs [cljs.nodejs :as nodejs])
            [clojure.string :as str])
  #?(:cljs (:require-macros [hitchhiker.tree.async :refer [case-async]]
                            [cljs.core.async.macros :refer [go]])))

#?(:cljs
   (enable-console-print!))

(defn iter-helper [tree key]
  (case-async
   :none
   (if-let [path (core/lookup-path tree key)]
     (msg/forward-iterator path)
     [])
   :core.async
   (go-try
    (let [iter-ch (async/chan)
          path (<? (core/lookup-path tree key))
          _  (when path
               (msg/forward-iterator iter-ch path key))
          v (<? iter-ch)
          _ (prn "value: " v)]
      (<? (async/into [] (async/to-chan (vector v))))))))

(deftest simple-konserve
  (testing "Insert and lookup"
    #?(:cljs
       (testing "NodeJS"
           (testing "filestore backend"
             (async done
                    (go-try
                     (let [folder    "/tmp/async-hitchhiker-tree-test"
                           _         (delete-store folder)
                           store     (kons/add-hitchhiker-tree-handlers
                                      (kc/ensure-cache (async/<!
                                                        (new-fs-store folder))))
                           backend   (kons/->KonserveBackend store)
                           init-tree (<? (core/reduce< (fn [t i] (msg/insert t i i))
                                                       (<? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                                       (range 1 11)))
                           flushed   (<? (core/flush-tree init-tree backend))
                           root-key  (kons/get-root-key (:tree flushed))
                           tree      (<? (kons/create-tree-from-root-key store root-key))]
                       (is (= (<? (msg/lookup tree -10)) nil))
                       (is (= (<? (msg/lookup tree 100)) nil))
                       (dotimes [i 10]
                         (is (= (<? (msg/lookup tree (inc i))) (inc i))))
                       (is (= (map first (<? (iter-helper tree 4))) (range 4 11)))
                       (is (= (map first (<? (iter-helper tree 0))) (range 1 11)))
                       (let [deleted  (<? (core/flush-tree (<? (msg/delete tree 3)) backend))
                             root-key (kons/get-root-key (:tree deleted))
                             tree     (<? (kons/create-tree-from-root-key store root-key))]
                         (is (= (<? (msg/lookup tree 2)) 2))
                         (is (= (<? (msg/lookup tree 3)) nil))
                         (is (= (<? (msg/lookup tree 4)) 4)))
                       (delete-store folder)
                       (done)))))
         (testing "in memory backend"
           (async done
                  (go-try
                   (let [store     (kons/add-hitchhiker-tree-handlers
                                    (kc/ensure-cache (async/<!
                                                      (new-mem-store))))
                         backend   (kons/->KonserveBackend store)
                         init-tree (<? (core/reduce< (fn [t i] (msg/insert t i i))
                                                     (<? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                                     (range 1 11)))
                         flushed   (<? (core/flush-tree init-tree backend))
                         root-key  (kons/get-root-key (:tree flushed))
                         tree      (<? (kons/create-tree-from-root-key store root-key))]
                     (is (= (<? (msg/lookup tree -10)) nil))
                     (is (= (<? (msg/lookup tree 100)) nil))
                     (dotimes [i 10]
                       (is (= (<? (msg/lookup tree (inc i))) (inc i))))
                     (is (= (map first (<? (iter-helper tree 4))) (range 4 11)))
                     (is (= (map first (<? (iter-helper tree 0))) (range 1 11)))
                     (let [deleted  (<? (core/flush-tree (<? (msg/delete tree 3)) backend))
                           root-key (kons/get-root-key (:tree deleted))
                           tree     (<? (kons/create-tree-from-root-key store root-key))]
                       (is (= (<? (msg/lookup tree 2)) 2))
                       (is (= (<? (msg/lookup tree 3)) nil))
                       (is (= (<? (msg/lookup tree 4)) 4)))
                     (done)))))))
    #?(:clj
       (testing "JVM"
         (testing "filestore backend"
           (let [folder   "/tmp/async-hitchhiker-tree-test"
                 _        (delete-store folder)
                 store    (kons/add-hitchhiker-tree-handlers
                           (kc/ensure-cache (async/<!! (new-fs-store folder :config {:fsync false}))))
                 backend  (kons/->KonserveBackend store)
                 flushed  (<?? (core/flush-tree
                                (time (reduce (fn [t i]
                                                (<?? (msg/insert t i i)))
                                              (<?? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                              (range 1 11)))
                                backend))
                 root-key (kons/get-root-key (:tree flushed))
                 tree     (<?? (kons/create-tree-from-root-key store root-key))]
             (is (= (<?? (msg/lookup tree -10)) nil))
             (is (= (<?? (msg/lookup tree 100)) nil))
             (dotimes [i 10]
               (is (= (<?? (msg/lookup tree (inc i))) (inc i))))
             (is (= (map first (msg/lookup-fwd-iter tree 4)) (range 4 11)))
             (is (= (map first (msg/lookup-fwd-iter tree 0)) (range 1 11)))
             (let [deleted  (<?? (core/flush-tree (<?? (msg/delete tree 3)) backend))
                   root-key (kons/get-root-key (:tree deleted))
                   tree     (<?? (kons/create-tree-from-root-key store root-key))]
               (is (= (<?? (msg/lookup tree 2)) 2))
               (is (= (<?? (msg/lookup tree 3)) nil))
               (is (= (<?? (msg/lookup tree 4)) 4)))
             (delete-store folder)))
         (testing "in memory backend"
           (let [store    (kons/add-hitchhiker-tree-handlers
                           (kc/ensure-cache (async/<!! (new-mem-store))))
                 backend  (kons/->KonserveBackend store)
                 flushed  (<?? (core/flush-tree
                                (time (reduce (fn [t i]
                                                (<?? (msg/insert t i i)))
                                              (<?? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                              (range 1 11)))
                                backend))
                 root-key (kons/get-root-key (:tree flushed))
                 tree     (<?? (kons/create-tree-from-root-key store root-key))]
             (is (= (<?? (msg/lookup tree -10)) nil))
             (is (= (<?? (msg/lookup tree 100)) nil))
             (dotimes [i 10]
               (is (= (<?? (msg/lookup tree (inc i))) (inc i))))
             (is (= (map first (msg/lookup-fwd-iter tree 4)) (range 4 11)))
             (is (= (map first (msg/lookup-fwd-iter tree 0)) (range 1 11)))
             (let [deleted  (<?? (core/flush-tree (<?? (msg/delete tree 3)) backend))
                   root-key (kons/get-root-key (:tree deleted))
                   tree     (<?? (kons/create-tree-from-root-key store root-key))]
               (is (= (<?? (msg/lookup tree 2)) 2))
               (is (= (<?? (msg/lookup tree 3)) nil))
               (is (= (<?? (msg/lookup tree 4)) 4)))))))))

;; adapted from redis tests
(defn insert
    [t k]
    (msg/insert t k k))

(defn ops-test [ops universe-size]
  (go-try
      (let [folder (str "/tmp/konserve-mixed-workload" (hasch/uuid))
            _ #?(:clj (delete-store folder) :cljs nil)
            store (kons/add-hitchhiker-tree-handlers
                   (kc/ensure-cache
                    #?(:clj (async/<!! (new-fs-store folder :config {:fsync false}))
                       :cljs (async/<! (new-mem-store)))))
            _ #?(:clj (assert (empty? (async/<!! (list-keys store)))
                              "Start with no keys")
                 :cljs nil)
                                        ;_ (swap! recorded-ops conj ops)
            [b-tree root set]
            (<? (core/reduce< (fn [[t root set] [op x]]
                                (go-try
                                    (let [x-reduced (when x (mod x universe-size))]
                                      (condp = op
                                        :flush (let [flushed (<? (core/flush-tree t (kons/->KonserveBackend store)))
                                                     t (:tree flushed)]
                                                 [t (<? (:storage-addr t)) set])
                                        :add [(<? (insert t x-reduced)) root (conj set x-reduced)]
                                        :del [(<? (msg/delete t x-reduced)) root (disj set x-reduced)]))))
                              [(<? (core/b-tree (core/->Config 3 3 2))) nil #{}]
                              ops))]
        (let [b-tree-order (seq (map first (<? (iter-helper b-tree -1))))
              res (= b-tree-order (seq (sort set)))]
          (assert res (str "These are unequal: " (pr-str b-tree-order) " " (pr-str (seq (sort set)))))
          res))))

;; TODO recheck when https://dev.clojure.org/jira/browse/TCHECK-128 is fixed
  ;; and share clj mixed-op-seq test, remove ops.cljc then.
  #?(:cljs
     (deftest manual-mixed-op-seq
       (async done
              (go-try
               (loop [[ops & r] recorded-ops]
                 (when ops
                   (is (<? (ops-test ops 1000)))
                   (recur r)))
               (done)))))

#?(:clj
     (defn mixed-op-seq
       "This is like the basic mixed-op-seq tests, but it also mixes in flushes to a konserve filestore"
       [add-freq del-freq flush-freq universe-size num-ops]
       (prop/for-all [ops (gen/vector (gen/frequency
                                       [[add-freq (gen/tuple (gen/return :add)
                                                             (gen/no-shrink gen/int))]
                                        [flush-freq (gen/return [:flush])]
                                        [del-freq (gen/tuple (gen/return :del)
                                                             (gen/no-shrink gen/int))]])
                                      num-ops)]
                     (<?? (ops-test ops universe-size)))))

#?(:clj
     (defspec test-many-keys-bigger-trees
       100
       (mixed-op-seq 800 200 10 1000 1000)))

#?(:cljs (run-tests))

(comment

  (def folder  "/tmp/async-hitchhiker-tree-test")

  (go (def store (kons/add-hitchhiker-tree-handlers
                  (kc/ensure-cache (async/<!
                                    (new-fs-store folder))))))

  (def backend (kons/->KonserveBackend store))

  (go (def init-tree (<? (core/reduce< (fn [t i] (msg/insert t i i))
                                       (<? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                       (range 1 11)))))

  (go (def flushed (<? (core/flush-tree init-tree backend))))

  (def root-key (kons/get-root-key (:tree flushed)))

  (go (def tree (<? (kons/create-tree-from-root-key store root-key))))


  (go (def path (<? (core/lookup-path tree 4))))

  (type (first path))

  (def it-ch (async/chan))

  (msg/forward-iterator it-ch path 4)

  (go (def v (<? it-ch)))

  v

  (go (prn (<? (async/into [] (async/to-chan (<? it-ch))))))

  (go (prn (<? (async/into [] (async/to-chan v)))))


  (go (prn (<? it-ch)))

  (go (prn (<? (async/into [] it-ch))))

  (go (prn (<? (go-try
                (let [iter-ch (async/chan)
                      path (<? (core/lookup-path tree 4))]
                  (when path
                    (msg/forward-iterator iter-ch path 4))
                  (<? (async/into [] iter-ch)))))))

  (go (prn (async/<! (async/into [1 2 3] (async/to-chan (range 10))))))

  (go (prn (async/<! (async/into [1 2 3]  (let [c (async/chan)
                                                _ (msg/forward-iterator c path 4)]
                                            (async/to-chan (<? c)))))))

  (type (async/to-chan (range 10)))

  (defn unsorted-array []
    (let [range (Math/floor (rand 100))]
      (loop [i 0
             a []]
        (if (= i range)
          a
          (recur (inc i) (conj a (Math/floor (rand 100))))))))

  (unsorted-array)











  )

(comment

  (def folder "/tmp/async-hitchhiker-tree-test")

  (delete-store folder)

  (def store    (kons/add-hitchhiker-tree-handlers
                 (kc/ensure-cache (async/<!! (new-fs-store folder :config {:fsync false})))))

  (def backend  (kons/->KonserveBackend store))

  (def flushed  (<?? (core/flush-tree
                      (time (reduce (fn [t i]
                                      (<?? (msg/insert t i i)))
                                    (<?? (core/b-tree (core/->Config 1 3 (- 3 1))))
                                    (range 1 11)))
                      backend)))

  (def root-key (kons/get-root-key (:tree flushed)))

  (def tree (<?? (kons/create-tree-from-root-key store root-key)))

  (def path (<?? (core/lookup-path tree 4)))

  (def it-ch (async/chan))

  (defn chan-seq [ch]
    (when-some [v (<?? ch)]
      (cons v (lazy-seq (chan-seq ch)))))

  (defn forward-iterator
    "Takes the result of a search and puts the iterated elements onto iter-ch
  going forward over the tree as needed. Does lg(n) backtracking sometimes."
    [iter-ch path start-key]
    (go-try
        (loop [path path]
          (if path
            (let  [_ (assert (core/data-node? (peek path)))
                   elements (drop-while (fn [[k v]]
                                          (neg? (core/compare k start-key)))
                                        (msg/apply-ops-in-path path))]
              (<? (async/onto-chan iter-ch elements false))
              (recur (<? (core/right-successor (pop path)))))
            (async/close! iter-ch)))))

  (defn lookup-fwd-iter
    "Compatibility helper to clojure sequences. Please prefer the channel
  interface of forward-iterator, as this function blocks your thread, which
  disturbs async contexts and might lead to poor performance. It is mainly here
  to facilitate testing or for exploration on the REPL."
    [tree key]
    (let [path (<?? (core/lookup-path tree key))
          iter-ch (async/chan)]
      (forward-iterator iter-ch path key)
      (chan-seq iter-ch)))

  (map #(async/<!! %) (lookup-fwd-iter tree 1))




  )
