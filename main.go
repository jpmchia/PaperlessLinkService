package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

func main() {
	config := loadConfig()
	log.Printf("Starting Paperless Link Service on port %s", config.Port)

	service, err := NewService(config)
	if err != nil {
		log.Fatalf("Failed to initialize service: %v", err)
	}
	defer service.db.Close()

	// Setup router
	router := mux.NewRouter()

	// API routes for custom field values
	customFieldValuesAPI := router.PathPrefix("/api/custom-field-values").Subrouter()
	customFieldValuesAPI.HandleFunc("/{fieldId:[0-9]+}/", service.handleGetFieldValues).Methods("GET")
	customFieldValuesAPI.HandleFunc("/{fieldId:[0-9]+}/search/", service.handleSearchFieldValues).Methods("GET")
	customFieldValuesAPI.HandleFunc("/{fieldId:[0-9]+}/counts/", service.handleGetValueCounts).Methods("POST")

	// API routes for custom views
	customViewsAPI := router.PathPrefix("/api/custom_views").Subrouter()
	customViewsAPI.HandleFunc("/", service.handleListCustomViews).Methods("GET")
	customViewsAPI.HandleFunc("/", service.handleCreateCustomView).Methods("POST")
	customViewsAPI.HandleFunc("/{id:[0-9]+}/", service.handleGetCustomView).Methods("GET")
	customViewsAPI.HandleFunc("/{id:[0-9]+}/", service.handleUpdateCustomView).Methods("PUT", "PATCH")
	customViewsAPI.HandleFunc("/{id:[0-9]+}/", service.handleDeleteCustomView).Methods("DELETE")

	// Health check
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := service.db.Ping(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}).Methods("GET")

	// CORS middleware
	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowedMethods([]string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization"}),
	)(router)

	// Setup server
	srv := &http.Server{
		Addr:         ":" + config.Port,
		Handler:      corsHandler,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	// Start server in goroutine
	go func() {
		log.Printf("Server listening on :%s", config.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
