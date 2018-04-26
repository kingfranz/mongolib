(ns mongolib.core
    (:require [utils.core :as utils]
              [slingshot.slingshot :refer [throw+ try+]]
              [taoensso.timbre :as log]
              [clj-time.local :as l]
              [clojure.spec.alpha :as s]
              [orchestra.core :refer [defn-spec]]
              [orchestra.spec.test :as st]
              [clojure.string :as str]
              [cheshire.core :refer :all]
              [monger.core :as mg]
              [monger.credentials :as mcr]
              [monger.collection :as mc]
              [monger.operators :refer :all]
              )
    (:import [java.util UUID]))

;;-----------------------------------------------------------------------------

(def ^:private db-connection (atom nil))
(def ^:private db-obj (atom nil))

(defn setup
    [db-name db-address db-user db-password]
    (let [credentials (mcr/create db-user db-name db-password)]
        (reset! db-connection (mg/connect-with-credentials db-address credentials))
        (reset! db-obj (mg/get-db @db-connection db-name))))

;;-----------------------------------------------------------------------------

(defn mk-id
    []
    (str (UUID/randomUUID)))

(defn mk-std-field
    []
    {:_id (mk-id) :created (l/local-now)})

;;-----------------------------------------------------------------------------

;monger.collection$find_one_as_map@5f2b4e24users
(defn-spec ^:private fname string?
    [s any?]
    (second (re-matches #"^[^$]+\$(.+)@.+$" (str s))))

(defn- do-mc
    [mc-func caller tbl & args]
    (log/trace (apply str caller ": " (fname mc-func) " " tbl " " (first args) "\n"))
    (let [ret (apply mc-func @db-obj tbl (first args))
          txt* (pr-str ret)
          txt  (if (> (count txt*) 500) (str (subs txt* 0 500) " ## and much more") txt*)]
        (log/trace caller "returned:" txt "\n")
        ret))

(defn mc-aggregate
    [func tbl & args]
    (do-mc mc/aggregate func tbl args))

(defn mc-find-maps
    [func tbl & args]
    (do-mc mc/find-maps func tbl args))

(defn mc-find-one-as-map
    [func tbl & args]
    (do-mc mc/find-one-as-map func tbl (vec args)))

(defn mc-find-map-by-id
    [func tbl & args]
    (do-mc mc/find-map-by-id func tbl args))

(defn mc-insert
    [func tbl & args]
    (do-mc mc/insert func tbl args))

(defn mc-insert-batch
    [func tbl & args]
    (do-mc mc/insert-batch func tbl args))

(defn mc-update
    [func tbl & args]
    (do-mc mc/update func tbl args))

(defn mc-update-by-id
    [func tbl & args]
    (do-mc mc/update-by-id func tbl args))

(defn mc-replace-by-id
    [func tbl & args]
    (do-mc mc/save func tbl args))

(defn mc-remove-by-id
    [func tbl & args]
    (do-mc mc/remove-by-id func tbl args))

;;-----------------------------------------------------------------------------

(st/instrument)
