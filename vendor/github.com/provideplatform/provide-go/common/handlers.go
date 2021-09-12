package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
)

const authorizationHeader = "Authorization"
const defaultCorsAccessControlAllowOrigin = "*"
const defaultCorsAccessControlAllowCredentials = "true"
const defaultCorsAccessControlAllowHeaders = "Accept, Accept-Encoding, Authorization, Cache-Control, Content-Length, Content-Type, Origin, User-Agent, X-CSRF-Token, X-Requested-With"
const defaultCorsAccessControlAllowMethods = "GET, POST, PUT, DELETE, OPTIONS"
const defaultCorsAccessControlExposeHeaders = "X-Total-Results-Count"
const defaultResponseContentType = "application/json; charset=UTF-8"
const defaultResultsPerPage = 25

// CORSMiddleware is a working middlware for using CORS with gin
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", defaultCorsAccessControlAllowOrigin)
		c.Writer.Header().Set("Access-Control-Allow-Credentials", defaultCorsAccessControlAllowCredentials)
		c.Writer.Header().Set("Access-Control-Allow-Headers", defaultCorsAccessControlAllowHeaders)
		c.Writer.Header().Set("Access-Control-Allow-Methods", defaultCorsAccessControlAllowMethods)
		c.Writer.Header().Set("Access-Control-Expose-Headers", defaultCorsAccessControlExposeHeaders)

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// Paginate the current request given the page number and results per page;
// returns the modified SQL query and adds x-total-results-count header to
// the response
func Paginate(c *gin.Context, db *gorm.DB, model interface{}) *gorm.DB {
	page := int64(1)
	rpp := int64(defaultResultsPerPage)

	if c.Query("page") != "" {
		if _page, err := strconv.ParseInt(c.Query("page"), 10, 64); err == nil {
			page = _page
		}
	}

	if c.Query("rpp") != "" {
		if _rpp, err := strconv.ParseInt(c.Query("rpp"), 10, 64); err == nil {
			rpp = _rpp
		}
	}

	query, totalResults := paginate(db, model, page, rpp)
	if totalResults != nil {
		c.Header("x-total-results-count", fmt.Sprintf("%d", *totalResults))
	}

	return query
}

// Paginate the given query given the page number and results per page;
// returns the update query and total results
func paginate(db *gorm.DB, model interface{}, page, rpp int64) (query *gorm.DB, totalResults *uint64) {
	db.Model(model).Count(&totalResults)
	query = db.Limit(rpp).Offset((page - 1) * rpp)
	return query, totalResults
}

// Render an object and status using the given gin context
func Render(obj interface{}, status int, c *gin.Context) {
	c.Header("content-type", defaultResponseContentType)
	c.Writer.WriteHeader(status)
	if &obj != nil && status != http.StatusNoContent {
		encoder := json.NewEncoder(c.Writer)
		encoder.SetIndent("", "    ")
		if err := encoder.Encode(obj); err != nil {
			panic(err)
		}
	} else {
		c.Header("content-length", "0")
	}
}

// RenderError writes an error message and status using the given gin context
func RenderError(message string, status int, c *gin.Context) {
	err := map[string]*string{}
	err["message"] = &message
	Render(err, status, c)
}

// RequireParams renders an error if any of the given parameters are not present in the given gin context
func RequireParams(requiredParams []string, c *gin.Context) error {
	var errs []string
	for _, param := range requiredParams {
		if c.Query(param) == "" {
			errs = append(errs, param)
		}
	}
	if len(errs) > 0 {
		msg := strings.Trim(fmt.Sprintf("missing required parameters: %s", strings.Join(errs, ", ")), " ")
		RenderError(msg, 400, c)
		return errors.New(msg)
	}
	return nil
}
