# datomic-queue

Queue data structure on top of datomic

## Usage

``` clojure

  (def client (d/client {:server-type :dev-local
                        :storage-dir :mem
                        :system "ci"}))

  (d/create-database client {:db-name "dm-queue"})
  (def conn (d/connect client {:db-name "dm-queue"}))
  (init-db conn)

  (def test-q (create-dbqueue conn))

  (push test-q "5") 
  (peek test-q)

  (peek-last test-q) 
  (peek-last (d/db conn) test-q)

  (pop test-q) 
```

## License

Copyright © 2023 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
