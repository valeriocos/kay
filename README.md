# kay
Move things around. In order to execute it, you should use: https://github.com/chaoss/grimoirelab-toolkit/pull/14

```
kay redis2es --redis-url redis://localhost/8 
             --es-url https://admin:admin@localhost:9200 
             --delay 10 
             --es-items-type perceval
```
