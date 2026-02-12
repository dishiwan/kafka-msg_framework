#!/bin/bash
# Create complete project structure

# Main directories
mkdir -p src/main/java/com/enterprise/messaging/{config,model,repository,service,consumer,publisher,validator,processor,performer,worker,cache,util,exception,controller,scheduler}
mkdir -p src/main/resources/{db/migration,sql,templates}
mkdir -p src/test/java/com/enterprise/messaging/{service,consumer,validator,processor,integration}
mkdir -p src/test/resources
mkdir -p docs/{diagrams,architecture}
mkdir -p scripts/{deployment,database}
mkdir -p config/{dev,qa,prod}

# Create marker files
touch src/main/java/com/enterprise/messaging/.gitkeep
touch src/test/java/com/enterprise/messaging/.gitkeep

echo "Project structure created successfully"
