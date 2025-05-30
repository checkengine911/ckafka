include docker/.env

BINARY_NAME=myapp

CMD_DIR=cmd/

TEST_DIR=./...

LINTER=golangci-lint

LINTER_PATH=$(GOPATH)/bin/$(LINTER)
LINTER_CONFIG=.golangci.yaml

GOOSE=goose
GOOSE_PATH=$(GOPATH)/bin/$(GOOSE)
MIGRATIONS_DIR=migrations

DB_CONN=host=localhost port=$(DB_PORT) user=$(DB_USER) dbname=$(DB_NAME) password=$(DB_PASS) sslmode=disable

# Define the default goal
.DEFAULT_GOAL := help

# The 'all' target will build the binary
all: build

# Compile the Go application
build: 
	go build -o $(BINARY_NAME) $(CMD_DIR)

# Run tests
test: 
	go test $(TEST_DIR)

# Lint the codebase
lint: $(LINTER_PATH) ## Lint the codebase
	$(LINTER_PATH) run -c $(LINTER_CONFIG)

# Ensure golangci-lint is installed
$(LINTER_PATH): 
	@command -v $(LINTER_PATH) >/dev/null 2>&1 || { \
		echo "golangci-lint not found, installing..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	}

# Clean up the built binary
clean: 
	rm -f $(BINARY_NAME)
	
# Start all services with Docker Compose and aply migrations
container: up 

# Start infrastructure with Docker Compose and apply migrations
container-dev: up-dev run 

# Run the application
run: 
	CONFIG_PATH=config/local.json go run cmd/main.go

# Start the services with Docker Compose
up: 
	COMPOSE_PROJECT_NAME=ckafkago docker compose -f docker/docker-compose.yaml --profile=test up -d --build 

# Start the infrastructure with Docker Compose
up-dev: 
	COMPOSE_PROJECT_NAME=ckafkago docker compose -f docker/docker-compose.yaml up -d --build

# Stop the services with Docker Compose
down: 
	COMPOSE_PROJECT_NAME=ckafkago docker compose -f docker/docker-compose.yaml --profile=test down

# Ensure goose is installed
check-goose: 
	@command -v $(GOOSE_PATH) >/dev/null 2>&1 || { \
		echo "goose not found, installing..."; \
		go install github.com/pressly/goose/v3/cmd/goose@latest; \
	}


# Create a new migration file
mc: check-goose 
	@read -p "Enter migration name: " NAME; \
	cd $(MIGRATIONS_DIR) && $(GOOSE_PATH) create $$NAME sql

# Apply all migrations
mu: check-goose 
	$(GOOSE_PATH) -dir $(MIGRATIONS_DIR) postgres "$(DB_CONN)" up

# Rollback the last migration
md: check-goose
	$(GOOSE_PATH) -dir $(MIGRATIONS_DIR) postgres "$(DB_CONN)" down

# Check migration status
ms: check-goose 
	$(GOOSE_PATH) -dir $(MIGRATIONS_DIR) postgres "$(DB_CONN)" status

# Migrate up to a specific version
muv: check-goose 
	@read -p "Enter migration version: " VERSION; \
	$(GOOSE_PATH) -dir $(MIGRATIONS_DIR) postgres "$(DB_CONN)" up $$VERSION

# Ensure swag is installed
check-swagger: 
	@command -v which swag >/dev/null 2>&1 || { \
		echo "swaggo not found, installing..."; \
		go install github.com/swaggo/swag/cmd/swag@latest; \
	}

# Initialize Swagger documentation
swagger-init: check-swagger 
	swag fmt && swag init --pdl=1 -g cmd/main.go -o docs/openapi

# Show this help message
help: 
	@cat docs/help.txt

.PHONY: all build test lint clean up up-dev down mc mu md ms muv check-goose check-swagger help
