(ns build
  "Build, install, and deploy tasks for ai.dyal/ducktape.

  Common entry points:
    clj -T:build jar         ; build target/ducktape-<version>.jar
    clj -T:build install     ; install to ~/.m2 for local consumption
    clj -T:build deploy      ; publish to Clojars
    clj -T:build clean       ; rm -rf target

  Version resolution order:
    1. :version key on the CLI (e.g. clj -T:build deploy :version '\"0.1.0\"')
    2. VERSION env var
    3. default-version below (snapshot)

  Deploy credentials (only required for `deploy`):
    CLOJARS_USERNAME — your Clojars username
    CLOJARS_PASSWORD — a Clojars deploy token (NOT your account password).
                       Create one at https://clojars.org/tokens and scope it
                       to ai.dyal/* for least-privilege."
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib              'ai.dyal/ducktape)
(def default-version  "0.1.0-SNAPSHOT")
(def class-dir        "target/classes")
(def src-dirs         ["src"])
(def github-url       "https://github.com/dynamic-alpha/ducktape")

(defn- resolve-version [opts]
  (or (:version opts)
      (System/getenv "VERSION")
      default-version))

(defn- jar-file [version]
  (format "target/%s-%s.jar" (name lib) version))

(defn- pom-data [version]
  [[:description "DuckDB bindings for tech.ml.dataset via Java Panama FFM."]
   [:url         github-url]
   [:licenses
    [:license
     [:name "MIT License"]
     [:url  "https://opensource.org/licenses/MIT"]]]
   [:developers
    [:developer
     [:name "Dynamic Alpha Technologies Inc."]]]
   [:scm
    [:url                 github-url]
    [:connection          "scm:git:https://github.com/dynamic-alpha/ducktape.git"]
    [:developerConnection "scm:git:ssh://git@github.com/dynamic-alpha/ducktape.git"]
    [:tag                 (str "v" version)]]])

(defn clean
  "Remove the build output directory."
  [_]
  (b/delete {:path "target"}))

(defn jar
  "Build the library jar in target/. Returns opts augmented with :version and :jar-file."
  [opts]
  (let [version (resolve-version opts)
        jar     (jar-file version)
        basis   (b/create-basis {:project "deps.edn"})]
    (clean nil)
    (b/write-pom {:class-dir class-dir
                  :lib       lib
                  :version   version
                  :basis     basis
                  :src-dirs  src-dirs
                  :pom-data  (pom-data version)})
    (b/copy-dir {:src-dirs   src-dirs
                 :target-dir class-dir})
    (b/jar {:class-dir class-dir
            :jar-file  jar})
    (println "Built" jar)
    (assoc opts :version version :jar-file jar)))

(defn install
  "Install the jar into the local Maven repo (~/.m2)."
  [opts]
  (let [{:keys [version jar-file]} (jar opts)
        basis                      (b/create-basis {:project "deps.edn"})]
    (b/install {:basis     basis
                :lib       lib
                :version   version
                :jar-file  jar-file
                :class-dir class-dir})
    (println "Installed" lib version "→ ~/.m2")))

(defn deploy
  "Build and deploy the jar to Clojars.
  Requires CLOJARS_USERNAME and CLOJARS_PASSWORD in the environment."
  [opts]
  (let [{:keys [version jar-file]} (jar opts)]
    (dd/deploy {:installer :remote
                :artifact  jar-file
                :pom-file  (b/pom-path {:lib lib :class-dir class-dir})})
    (println "Deployed" lib version "→ Clojars")))
