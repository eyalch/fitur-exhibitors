#!/usr/bin/env bb

(ns script
  (:require [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [org.httpkit.client :as http]
            [cheshire.core :as json]))

(defn request [body callback]
  (http/request
    {:url     "https://api.swapcard.com/graphql"
     :method  :post
     :headers {"Content-Type"       "application/json"
               "X-Content-Language" "en_US"}
     :body    (json/generate-string body)}
    #(-> % :body (json/parse-string true) callback)))

(defn get-exhibitor [id]
  (log/info "Getting exhibitor" {:id id})
  (request
    {:operationName "EventExhibitorDetailsViewQuery"
     :variables     {:withEvent    true
                     :skipMeetings true
                     :exhibitorId  id
                     :eventId      "RXZlbnRfOTY5MjQx"}
     :extensions    {:persistedQuery {:version    1
                                      :sha256Hash "688983dc267a87f6ba926c432f43d264b3ed3b40363bb3472ffb71ee75f292b1"}}}
    (fn [data]
      (log/info "Got exhibitor" {:id id})
      (let [exhibitor (get-in data [:data :exhibitor])
            fields (get-in exhibitor [:withEvent :fields])
            get-field (fn [name] (some #(when (= (:name %) name) %) fields))]
        {:name            (:name exhibitor)
         :activity-sector (-> (get-field "Activity sector") (get-in [:values 0 :text]))
         :country         (-> (get-field "Country") (get-in [:value :text]))
         :province-spain  (-> (get-field "Province (Spain)") (get-in [:value :text]))
         :address         (->> [:place :street :zipCode :city :state :country]
                               (map (:address exhibitor))
                               (remove nil?)
                               (str/join ", "))
         :website         (:websiteUrl exhibitor)
         :phone           (->> exhibitor :phoneNumbers (map :formattedNumber) (str/join ", "))
         :email           (:email exhibitor)}))))

(defn get-exhibitors-page [end-cursor]
  (log/info "Getting exhibitors page" {:end-cursor end-cursor})
  (request
    {:operationName "EventExhibitorListViewConnectionQuery"
     :variables     (cond-> {:withEvent       true
                             :viewId          "RXZlbnRWaWV3XzM3NDg2OA=="
                             :eventId         "RXZlbnRfOTY5MjQx"
                             :selectedFilters [{:mustEventFiltersIn [{:filterId "RmllbGREZWZpbml0aW9uXzE5NDYyMQ=="
                                                                      :values   ["RmllbGRWYWx1ZV8xMzUyNjUzNA=="
                                                                                 "RmllbGRWYWx1ZV8xMzUyNjU2Mw=="
                                                                                 "RmllbGRWYWx1ZV8xMzUyNjU3MA=="]}]}]}
                            end-cursor (assoc :endCursor end-cursor))
     :extensions    {:persistedQuery {:version    1
                                      :sha256Hash "b7d34d371267414bb97bcb6561e50248d030523308a211765fe3f69f4b506569"}}}
    (fn [data]
      (let [{:keys [nodes pageInfo]} (get-in data [:data :view :exhibitors])
            ids (map :id nodes)
            {has-next-page :hasNextPage next-end-cursor :endCursor} pageInfo]
        (log/info "Got exhibitors page" {:end-cursor    end-cursor
                                         :ids-count     (count ids)
                                         :has-next-page has-next-page})
        {:ids           ids
         :has-next-page has-next-page
         :end-cursor    next-end-cursor}))))

(defn -main [& _args]
  (with-open [writer (io/writer "fitur_exhibitors.csv")]
    (letfn [(write-row [row] (csv/write-csv writer [row]))]
      (write-row ["Name" "Activity sector" "Country" "Province (Spain)" "Address" "Website" "Phone" "Email"])
      (loop [end-cursor nil]
        (let [{:keys [ids has-next-page end-cursor]} @(get-exhibitors-page end-cursor)
              exhibitors (->> ids (map get-exhibitor) doall)]
          (doseq [exhibitor exhibitors]
            (write-row (map @exhibitor [:name :activity-sector :country :province-spain :address :website :phone :email])))
          (when has-next-page
            (recur end-cursor)))))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
