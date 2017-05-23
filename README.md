# getwelldoc

A [reagent](https://github.com/reagent-project/reagent) application designed to ... well, that part is up to you.

## Firebird

1. Download the driver package from here:
   http://firebirdsql.org/en/jdbc-driver/
   (I used Jaybird-2.2.10-JDK_1.8.zip)

2. Explode the zip file to a temp dir, e.g., ~/tmp/jaybird
   Note: Unlike most distributions, this zip does not include the
         root 'jaybird' folder.

3. Edit (probably new file)  ~/.lein/profiles.clj
   Add this line:
{:user {:plugins [[lein-localrepo "0.5.3"]]}}

4. Run these commands:

     cd ~/asi/src/tao2/winglue-well
     lein localrepo install ~/tmp/jaybird/jaybird-full-2.2.10.jar org/firebirdsql/jdbc 2.2.10

5. In project.clj, review the dependencies, it should include:

    [org/firebirdsql/jdbc "2.2.10"]

6. See resources/tao2-config.clj for examples of configuring data source url

## Development Mode

### Run application:

```
lein clean
lein figwheel dev
```

Figwheel will automatically push cljs changes to the browser.

Wait a bit, then browse to [http://localhost:3449](http://localhost:3449).

## Production Build

```
lein clean
lein cljsbuild once min
```
