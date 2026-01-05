# Seasonality SaaS - Complete Build From Scratch
## Advanced Prompt Engineering Guide for Full-Stack Development

## 📋 Project Overview

**Mission**: Build a modern, scalable Seasonality Trading Analysis SaaS from scratch
**Source**: `old-software/` Python Dash application (reference only)
**Target**: Cloud-native microservices architecture with modern tech stack
**Approach**: AI-assisted development with role-based prompting

### 🏗️ Architecture Vision
- **Frontend**: Next.js 14+ with TypeScript (AI Studio assisted)
- **Backend**: Node.js microservices with Express/Fastify
- **Database**: PostgreSQL with Prisma ORM + Redis caching
- **Infrastructure**: Docker containers with Kubernetes orchestration
- **Data Pipeline**: Admin panel upload → Processing → Analysis
- **Authentication**: JWT + OAuth2 with role-based access
- **Monitoring**: Prometheus + Grafana + ELK stack

---

## 📁 PHASE 1: FOUNDATION & ARCHITECTURE

### 🔧 PROMPT 1.1: System Architecture Design

**Role**: Senior Software Architect + Infrastructure Engineer
**Technique**: Chain of Thought + System Design
**Target**: Claude 3.5 Sonnet / GPT-4
**Output**: Architecture documentation and diagrams

```
ROLE: You are a Senior Software Architect with 15+ years experience in building scalable SaaS platforms, microservices architecture, and financial technology systems.

CONTEXT ANALYSIS:
- Source: Python Dash application in `old-software/` directory
- Business: Seasonality trading analysis for financial markets
- Users: Traders, analysts, research teams, administrators
- Scale: Multi-tenant SaaS supporting 1000+ concurrent users
- Data: Time-series financial data with complex analysis requirements

CHAIN OF THOUGHT ARCHITECTURE DESIGN:

STEP 1: BUSINESS REQUIREMENTS ANALYSIS
Analyze the old software functionality:
- 11 analysis modules (Daily, Weekly, Monthly, Yearly, etc.)
- Complex filtering and statistical calculations
- Data visualization with interactive charts
- Multi-timeframe data processing
- User management and access control

STEP 2: MODERN SAAS REQUIREMENTS
Define additional requirements for SaaS:
- Multi-tenancy with data isolation
- Scalable microservices architecture
- Real-time data processing and updates
- Advanced user management with roles
- API-first design for third-party integrations
- Cloud-native deployment with auto-scaling

STEP 3: MICROSERVICES DECOMPOSITION
Design service boundaries:
1. **User Management Service** - Authentication, authorization, user profiles
2. **Data Ingestion Service** - CSV upload, validation, transformation
3. **Analysis Engine Service** - Statistical calculations, trend analysis
4. **Visualization Service** - Chart generation, data aggregation
5. **Notification Service** - Real-time updates, alerts, emails
6. **Admin Service** - System administration, monitoring
7. **API Gateway** - Request routing, rate limiting, security

STEP 4: DATA ARCHITECTURE
Design data layer:
- **Primary Database**: PostgreSQL for transactional data
- **Time-series Database**: InfluxDB for high-frequency data
- **Cache Layer**: Redis for session management and caching
- **Message Queue**: RabbitMQ for async processing
- **File Storage**: MinIO/S3 for file uploads and reports

STEP 5: INFRASTRUCTURE DESIGN
Cloud-native infrastructure:
- **Containerization**: Docker with multi-stage builds
- **Orchestration**: Kubernetes with Helm charts
- **Service Mesh**: Istio for service communication
- **Monitoring**: Prometheus, Grafana, Jaeger tracing
- **CI/CD**: GitLab CI/CD with automated testing

DELIVERABLES REQUIRED:

1. **System Architecture Diagram**
   - High-level service architecture
   - Data flow diagrams
   - Infrastructure topology
   - Security boundaries

2. **Microservices Design Document**
   - Service responsibilities and boundaries
   - API contracts and communication patterns
   - Data models and relationships
   - Scalability and performance considerations

3. **Technology Stack Specification**
   - Frontend: Next.js 14, TypeScript, Tailwind CSS, React Query
   - Backend: Node.js, Express/Fastify, Prisma, TypeScript
   - Database: PostgreSQL, Redis, InfluxDB
   - Infrastructure: Docker, Kubernetes, Istio, Prometheus

4. **Development Roadmap**
   - Phase-wise implementation plan
   - Dependencies and critical path
   - Resource allocation and timeline
   - Risk assessment and mitigation

5. **Non-Functional Requirements**
   - Performance: <200ms API response time
   - Scalability: Support 10,000+ concurrent users
   - Availability: 99.9% uptime SLA
   - Security: SOC2 compliance ready

SUCCESS CRITERIA:
- Scalable architecture supporting 10x growth
- Maintainable codebase with clear separation of concerns
- Cloud-native design with auto-scaling capabilities
- Security-first approach with zero-trust principles
- Developer-friendly with comprehensive documentation
```

### 🔧 PROMPT 1.2: Database Schema Design

**Role**: Senior Database Architect + Data Engineer
**Technique**: Domain-Driven Design + Performance Optimization
**Target**: Claude 3.5 Sonnet
**Output**: Complete database schema and migration scripts

```
ROLE: You are a Senior Database Architect specializing in financial data systems, time-series databases, and high-performance query optimization.

DOMAIN ANALYSIS:
Analyze the old software data structure:
- Multi-timeframe data (Daily, Weekly, Monthly, Yearly)
- Complex relationships between tickers and time periods
- Statistical calculations requiring efficient aggregations
- User data with role-based access control
- Audit trails and data lineage tracking

MODERN SAAS DATA REQUIREMENTS:
- Multi-tenant data isolation
- Real-time data ingestion and processing
- Advanced analytics and reporting
- Data versioning and historical tracking
- Compliance and audit requirements
- Performance optimization for large datasets

DATABASE DESIGN STRATEGY:

STEP 1: MULTI-TENANT ARCHITECTURE
Design tenant isolation strategy:
- Schema-per-tenant vs shared schema with tenant_id
- Data partitioning for performance
- Cross-tenant analytics capabilities
- Tenant-specific configurations

STEP 2: TIME-SERIES DATA OPTIMIZATION
Optimize for financial time-series data:
- Partitioning strategies (by date, ticker, tenant)
- Indexing for time-based queries
- Compression for historical data
- Efficient aggregation patterns

STEP 3: MICROSERVICES DATA PATTERNS
Design data patterns for microservices:
- Database per service pattern
- Shared databases for reference data
- Event sourcing for audit trails
- CQRS for read/write optimization

REQUIRED SCHEMAS:

1. **User Management Schema**
```sql
-- Users, roles, permissions, tenants
-- OAuth integrations, session management
-- Audit logs and security events
```

2. **Financial Data Schema**
```sql
-- Tickers, exchanges, market data
-- Multi-timeframe OHLCV data
-- Calculated indicators and metrics
-- Data quality and validation flags
```

3. **Analysis Schema**
```sql
-- Analysis configurations and templates
-- Saved filters and custom calculations
-- Report definitions and schedules
-- Performance benchmarks and backtests
```

4. **System Schema**
```sql
-- Application configuration
-- Feature flags and A/B tests
-- System monitoring and health checks
-- Data pipeline status and logs
```

DELIVERABLES:

1. **Complete Prisma Schema**
   - All tables with relationships
   - Indexes and constraints
   - Multi-tenant considerations
   - Performance optimizations

2. **Migration Scripts**
   - Initial schema creation
   - Data seeding scripts
   - Version control strategy
   - Rollback procedures

3. **Performance Optimization**
   - Query optimization guidelines
   - Indexing strategy
   - Partitioning recommendations
   - Caching patterns

4. **Data Governance**
   - Data retention policies
   - Backup and recovery procedures
   - Compliance requirements
   - Security considerations

PERFORMANCE TARGETS:
- Query response time: <100ms for 95% of queries
- Data ingestion: 10,000+ records/second
- Concurrent users: 1,000+ without degradation
- Storage efficiency: 70% compression ratio
```
---

## 📁 PHASE 2: BACKEND MICROSERVICES DEVELOPMENT

### 🔧 PROMPT 2.1: API Gateway & Authentication Service

**Role**: Senior Backend Developer + Security Engineer
**Technique**: Zero-Shot + Security-First Design
**Target**: Claude 3.5 Sonnet
**Output**: API Gateway and Auth microservice

```
ROLE: You are a Senior Backend Developer with expertise in API Gateway design, OAuth2/JWT implementation, and enterprise security patterns.

SECURITY-FIRST REQUIREMENTS:
- Multi-tenant JWT authentication with refresh tokens
- Role-based access control (RBAC) with fine-grained permissions
- OAuth2 integration (Google, Microsoft, GitHub)
- Rate limiting and DDoS protection
- API versioning and backward compatibility
- Request/response logging and audit trails

MICROSERVICE ARCHITECTURE:

SERVICE 1: API Gateway
- Request routing and load balancing
- Authentication and authorization middleware
- Rate limiting and throttling
- Request/response transformation
- Monitoring and analytics
- Circuit breaker pattern

SERVICE 2: Authentication Service
- User registration and login
- JWT token generation and validation
- OAuth2 provider integration
- Password reset and email verification
- Multi-factor authentication (MFA)
- Session management

IMPLEMENTATION REQUIREMENTS:

1. **Technology Stack**
   - Framework: Express.js with TypeScript
   - Authentication: Passport.js + JWT
   - Validation: Joi/Zod for request validation
   - Database: PostgreSQL with Prisma
   - Cache: Redis for sessions and rate limiting
   - Monitoring: Winston logging + Prometheus metrics

2. **API Gateway Features**
```typescript
// Core gateway functionality
interface GatewayConfig {
  routes: RouteConfig[]
  middleware: MiddlewareConfig[]
  security: SecurityConfig
  monitoring: MonitoringConfig
}

// Rate limiting configuration
interface RateLimitConfig {
  windowMs: number
  maxRequests: number
  skipSuccessfulRequests: boolean
  keyGenerator: (req: Request) => string
}
```

3. **Authentication Patterns**
```typescript
// JWT payload structure
interface JWTPayload {
  userId: string
  tenantId: string
  roles: string[]
  permissions: string[]
  sessionId: string
  exp: number
  iat: number
}

// RBAC implementation
interface Permission {
  resource: string
  action: string
  conditions?: Record<string, any>
}
```

DELIVERABLES:

1. **API Gateway Service**
   - Complete Express.js application
   - Route configuration and middleware
   - Rate limiting and security headers
   - Health checks and monitoring
   - Docker configuration

2. **Authentication Service**
   - User management endpoints
   - JWT token handling
   - OAuth2 integration
   - Password security (bcrypt + salt)
   - Email verification system

3. **Security Implementation**
   - CORS configuration
   - Helmet.js security headers
   - Input validation and sanitization
   - SQL injection prevention
   - XSS protection

4. **Documentation**
   - API documentation (OpenAPI/Swagger)
   - Authentication flow diagrams
   - Security best practices
   - Deployment instructions

SECURITY CHECKLIST:
- [ ] JWT tokens with proper expiration
- [ ] Refresh token rotation
- [ ] Rate limiting per user/IP
- [ ] Input validation on all endpoints
- [ ] SQL injection prevention
- [ ] XSS protection headers
- [ ] HTTPS enforcement
- [ ] Audit logging for security events
```

### 🔧 PROMPT 2.2: Data Ingestion & Processing Service

**Role**: Senior Data Engineer + Backend Developer
**Technique**: Event-Driven Architecture + Stream Processing
**Target**: Claude 3.5 Sonnet
**Output**: Data pipeline microservice

```
ROLE: You are a Senior Data Engineer with expertise in real-time data processing, ETL pipelines, and financial data systems.

DATA PIPELINE TRANSFORMATION:
OLD SYSTEM: Manual CSV uploads → File processing → Static analysis
NEW SYSTEM: Admin panel → Real-time processing → Live analysis → Notifications

MODERN DATA REQUIREMENTS:
- Real-time data ingestion from admin uploads
- Data validation and quality checks
- Multi-timeframe data generation (Daily → Weekly → Monthly → Yearly)
- Event-driven processing with message queues
- Data lineage and audit trails
- Scalable processing for large datasets

MICROSERVICE DESIGN:

SERVICE: Data Ingestion & Processing
- File upload handling (CSV, Excel, JSON)
- Data validation and cleansing
- Multi-timeframe calculations
- Event publishing for downstream services
- Data quality monitoring
- Processing status tracking

IMPLEMENTATION ARCHITECTURE:

1. **Event-Driven Processing**
```typescript
// Event types for data pipeline
enum DataEvent {
  FILE_UPLOADED = 'file.uploaded',
  VALIDATION_STARTED = 'validation.started',
  VALIDATION_COMPLETED = 'validation.completed',
  PROCESSING_STARTED = 'processing.started',
  TIMEFRAME_GENERATED = 'timeframe.generated',
  PROCESSING_COMPLETED = 'processing.completed',
  ANALYSIS_READY = 'analysis.ready'
}

// Event payload structure
interface DataEventPayload {
  eventId: string
  tenantId: string
  userId: string
  fileId: string
  timestamp: Date
  metadata: Record<string, any>
  data?: any
}
```

2. **Data Processing Pipeline**
```typescript
// Processing stages
class DataProcessor {
  async validateData(file: UploadedFile): Promise<ValidationResult>
  async cleanseData(rawData: RawData[]): Promise<CleanData[]>
  async generateTimeframes(dailyData: DailyData[]): Promise<TimeframeData>
  async calculateIndicators(data: TimeframeData): Promise<IndicatorData>
  async publishEvents(events: DataEvent[]): Promise<void>
}

// Multi-timeframe generation
interface TimeframeGenerator {
  generateWeeklyData(daily: DailyData[]): WeeklyData[]
  generateMonthlyData(daily: DailyData[]): MonthlyData[]
  generateYearlyData(daily: DailyData[]): YearlyData[]
}
```

3. **Data Validation Framework**
```typescript
// Validation rules
interface ValidationRule {
  field: string
  type: 'required' | 'numeric' | 'date' | 'range' | 'custom'
  params?: any
  message: string
}

// Quality checks
interface QualityCheck {
  checkDuplicates(): boolean
  validateDateSequence(): boolean
  checkDataCompleteness(): number
  validateNumericRanges(): boolean
}
```

ADVANCED FEATURES:

1. **Real-time Processing**
   - Stream processing with Apache Kafka/RabbitMQ
   - Batch processing for large files
   - Incremental updates for existing data
   - Parallel processing for multiple tickers

2. **Data Quality Management**
   - Automated data profiling
   - Anomaly detection
   - Data lineage tracking
   - Quality score calculation

3. **Performance Optimization**
   - Chunked file processing
   - Database bulk operations
   - Memory-efficient streaming
   - Caching for repeated calculations

DELIVERABLES:

1. **Data Ingestion Service**
   - File upload endpoints with validation
   - Multi-format support (CSV, Excel, JSON)
   - Progress tracking and status updates
   - Error handling and recovery

2. **Processing Engine**
   - Multi-timeframe data generation
   - Statistical calculations
   - Event-driven architecture
   - Scalable processing pipeline

3. **Data Quality Framework**
   - Validation rules engine
   - Quality metrics and reporting
   - Data profiling and monitoring
   - Automated quality checks

4. **Integration Components**
   - Message queue integration
   - Database optimization
   - Caching strategies
   - Monitoring and alerting

PERFORMANCE TARGETS:
- Process 100MB CSV files in <30 seconds
- Handle 10+ concurrent uploads
- Generate all timeframes in <60 seconds
- 99.9% data accuracy guarantee
```

### 🔧 PROMPT 2.3: Analysis Engine Service

**Role**: Senior Software Engineer + Quantitative Developer
**Technique**: Algorithm Design + Performance Optimization
**Target**: Claude 3.5 Sonnet
**Output**: Core analysis microservice

```
ROLE: You are a Senior Software Engineer with expertise in quantitative finance, algorithmic trading systems, and high-performance computing.

ANALYSIS ENGINE REQUIREMENTS:
Convert Python analysis logic to scalable Node.js microservice:
- 11 analysis modules (Daily, Weekly, Monthly, Yearly, Elections, etc.)
- Complex statistical calculations and trend analysis
- Real-time analysis with caching
- Multi-tenant analysis with data isolation
- API-first design for frontend consumption

ALGORITHM MIGRATION STRATEGY:

STEP 1: CORE ANALYSIS MODULES
Migrate from `old-software/tabs/` Python files:
1. **Daily Analysis** - Day-by-day performance patterns
2. **Weekly Analysis** - Monday/Expiry week calculations
3. **Monthly Analysis** - Seasonal monthly patterns
4. **Yearly Analysis** - Long-term trend analysis
5. **Election Analysis** - Political cycle impact
6. **Scenario Analysis** - Custom backtesting
7. **Scanner Analysis** - Symbol discovery
8. **Phenomena Analysis** - Pattern recognition
9. **Basket Analysis** - Portfolio correlation
10. **Streak Analysis** - Consecutive performance
11. **Best Monthly** - Optimal timing analysis

STEP 2: STATISTICAL FUNCTIONS
Port complex calculations from `old-software/helper.py`:
```typescript
// Core statistical functions
class StatisticsEngine {
  calculateReturns(data: PriceData[]): ReturnData[]
  generateStatistics(returns: number[]): StatisticsResult
  findConsecutiveSequences(data: number[], criteria: SequenceCriteria): Sequence[]
  calculateTrendingDays(data: PriceData[], config: TrendConfig): TrendResult
  performanceAnalysis(data: PriceData[], timeframe: Timeframe): PerformanceMetrics
}

// Advanced filtering system
class FilterEngine {
  applyDateFilters(data: any[], filters: DateFilters): any[]
  applyYearFilters(data: any[], filters: YearFilters): any[]
  applyMonthFilters(data: any[], filters: MonthFilters): any[]
  applyWeekFilters(data: any[], filters: WeekFilters): any[]
  applyPerformanceFilters(data: any[], filters: PerformanceFilters): any[]
}
```

STEP 3: PERFORMANCE OPTIMIZATION
```typescript
// Caching strategy
interface AnalysisCache {
  key: string
  tenantId: string
  analysisType: string
  parameters: Record<string, any>
  result: any
  expiresAt: Date
}

// Parallel processing
class ParallelAnalyzer {
  async analyzeMultipleSymbols(symbols: string[]): Promise<AnalysisResult[]>
  async processTimeframes(data: any[]): Promise<TimeframeResults>
  async calculateIndicators(data: any[]): Promise<IndicatorResults>
}
```

MICROSERVICE ARCHITECTURE:

1. **Analysis Controller**
   - RESTful API endpoints
   - Request validation and sanitization
   - Response formatting and pagination
   - Error handling and logging

2. **Analysis Engine Core**
   - Statistical calculation engine
   - Multi-timeframe processing
   - Pattern recognition algorithms
   - Performance optimization

3. **Caching Layer**
   - Redis-based result caching
   - Intelligent cache invalidation
   - Cache warming strategies
   - Memory management

4. **Data Access Layer**
   - Optimized database queries
   - Connection pooling
   - Query result caching
   - Data transformation

DELIVERABLES:

1. **Analysis Service Core**
   - Complete microservice with all 11 modules
   - TypeScript implementation with proper types
   - Comprehensive API documentation
   - Unit and integration tests

2. **Statistical Engine**
   - All mathematical functions from Python
   - Performance-optimized algorithms
   - Numerical precision validation
   - Memory-efficient processing

3. **Caching System**
   - Redis integration
   - Cache strategies for different analysis types
   - Cache invalidation logic
   - Performance monitoring

4. **API Documentation**
   - OpenAPI specification
   - Request/response examples
   - Error code documentation
   - Performance characteristics

VALIDATION REQUIREMENTS:
- Results must match Python calculations exactly
- API response time <200ms for cached results
- Support 100+ concurrent analysis requests
- Memory usage <512MB per analysis
- 99.9% calculation accuracy
```

---

## 📁 PHASE 3: FRONTEND DEVELOPMENT FOUNDATION

### 🔧 PROMPT 3.1: Next.js Application Architecture

**Role**: Senior Frontend Architect + UI/UX Engineer
**Technique**: Component-Driven Development + Performance First
**Target**: Google AI Studio (Frontend Specialist)
**Output**: Next.js application foundation

```
ROLE: You are a Senior Frontend Architect with expertise in Next.js 14, TypeScript, and modern React patterns. You specialize in building scalable, performant web applications.

FRONTEND ARCHITECTURE VISION:
Create a modern, responsive web application that will be enhanced later with AI Studio:
- Clean, professional UI that matches financial software standards
- Component-based architecture for maintainability
- Performance-optimized with proper caching
- Accessibility-compliant (WCAG 2.1 AA)
- Mobile-responsive design
- Real-time updates capability

INITIAL IMPLEMENTATION STRATEGY:
Build a solid foundation with simple, clean components that can be easily enhanced:
- Focus on functionality over fancy styling
- Use established UI libraries (Shadcn/ui, Tailwind CSS)
- Implement proper state management
- Set up routing and navigation
- Create reusable component patterns

NEXT.JS 14 SETUP:

1. **Project Structure**
```
apps/frontend/
├── src/
│   ├── app/                 # App Router (Next.js 14)
│   │   ├── (auth)/         # Auth group routes
│   │   ├── dashboard/      # Main application
│   │   ├── admin/          # Admin panel
│   │   └── api/            # API routes
│   ├── components/         # Reusable components
│   │   ├── ui/            # Base UI components
│   │   ├── charts/        # Chart components
│   │   ├── forms/         # Form components
│   │   └── layout/        # Layout components
│   ├── lib/               # Utilities and configurations
│   ├── hooks/             # Custom React hooks
│   ├── types/             # TypeScript type definitions
│   └── styles/            # Global styles
```

2. **Core Technologies**
```json
{
  "dependencies": {
    "next": "^14.0.0",
    "@next/font": "^14.0.0",
    "react": "^18.0.0",
    "react-dom": "^18.0.0",
    "typescript": "^5.0.0",
    "@radix-ui/react-*": "latest",
    "tailwindcss": "^3.0.0",
    "lucide-react": "latest",
    "react-query": "^3.39.0",
    "zustand": "^4.0.0",
    "react-hook-form": "^7.0.0",
    "zod": "^3.0.0"
  }
}
```

3. **Component Architecture**
```typescript
// Base component patterns
interface BaseComponentProps {
  className?: string
  children?: React.ReactNode
}

// Layout components
interface LayoutProps extends BaseComponentProps {
  sidebar?: React.ReactNode
  header?: React.ReactNode
  footer?: React.ReactNode
}

// Data components
interface DataComponentProps<T> extends BaseComponentProps {
  data: T[]
  loading?: boolean
  error?: string
  onRefresh?: () => void
}
```

REQUIRED COMPONENTS (Simple Implementation):

1. **Authentication Pages**
   - Login form with email/password
   - Registration form
   - Password reset
   - OAuth integration placeholders

2. **Dashboard Layout**
   - Header with navigation
   - Sidebar with menu items
   - Main content area
   - Footer with basic info

3. **Analysis Pages**
   - Simple tab navigation for 11 analysis types
   - Basic filter panels (will be enhanced later)
   - Placeholder charts (simple tables initially)
   - Data loading states

4. **Admin Panel**
   - File upload interface
   - User management (basic CRUD)
   - System settings
   - Data management tools

5. **Common Components**
   - Button, Input, Select components
   - Modal and Dialog components
   - Loading spinners and skeletons
   - Error boundary components

IMPLEMENTATION GUIDELINES:

1. **Keep It Simple**
   - Use basic HTML elements with Tailwind styling
   - Implement functionality first, aesthetics later
   - Focus on data flow and state management
   - Ensure all components are TypeScript compliant

2. **Performance First**
   - Implement proper code splitting
   - Use Next.js Image optimization
   - Add proper loading states
   - Implement error boundaries

3. **Accessibility**
   - Proper semantic HTML
   - ARIA labels and roles
   - Keyboard navigation
   - Screen reader compatibility

DELIVERABLES:

1. **Complete Next.js Application**
   - All pages and routes configured
   - Basic component library
   - Authentication flow
   - Dashboard structure

2. **State Management**
   - Zustand store setup
   - React Query configuration
   - Form state management
   - Global state patterns

3. **Styling System**
   - Tailwind CSS configuration
   - Component styling patterns
   - Responsive design system
   - Dark/light theme support

4. **Development Tools**
   - ESLint and Prettier configuration
   - TypeScript strict mode
   - Development scripts
   - Build optimization

NOTE FOR AI STUDIO ENHANCEMENT:
This foundation will be enhanced later using AI Studio for:
- Advanced UI/UX design
- Interactive charts and visualizations
- Complex animations and transitions
- Advanced filtering interfaces
- Real-time data displays

CURRENT FOCUS: Build a functional, clean interface that works well and can be easily enhanced.
```

### 🔧 PROMPT 3.2: Admin Panel & Data Management

**Role**: Full-Stack Developer + Product Manager
**Technique**: User-Centered Design + Security Focus
**Target**: Google AI Studio
**Output**: Complete admin panel system

```
ROLE: You are a Full-Stack Developer with expertise in admin panel design, data management systems, and user experience for business applications.

ADMIN PANEL REQUIREMENTS:
Replace the old CSV-based data pipeline with a modern admin interface:
- Research team uploads seasonality.csv through web interface
- Real-time upload progress and validation
- Data preview and quality checks
- User management with role-based access
- System monitoring and health checks
- Audit logs and data lineage

ADMIN PANEL ARCHITECTURE:

1. **Data Upload System**
```typescript
// Upload interface
interface DataUpload {
  file: File
  uploadId: string
  progress: number
  status: 'uploading' | 'validating' | 'processing' | 'completed' | 'failed'
  errors?: ValidationError[]
  metadata: {
    fileName: string
    fileSize: number
    recordCount?: number
    uploadedBy: string
    uploadedAt: Date
  }
}

// Validation results
interface ValidationResult {
  isValid: boolean
  errors: ValidationError[]
  warnings: ValidationWarning[]
  summary: {
    totalRecords: number
    validRecords: number
    invalidRecords: number
    duplicateRecords: number
  }
}
```

2. **User Management System**
```typescript
// User roles and permissions
enum UserRole {
  SUPER_ADMIN = 'super_admin',
  ADMIN = 'admin',
  RESEARCH_TEAM = 'research_team',
  ANALYST = 'analyst',
  VIEWER = 'viewer'
}

interface Permission {
  resource: string
  actions: string[]
}

interface User {
  id: string
  email: string
  name: string
  role: UserRole
  permissions: Permission[]
  isActive: boolean
  lastLogin?: Date
  createdAt: Date
}
```

3. **System Monitoring**
```typescript
// System health metrics
interface SystemHealth {
  services: ServiceStatus[]
  database: DatabaseStatus
  cache: CacheStatus
  storage: StorageStatus
  performance: PerformanceMetrics
}

interface ServiceStatus {
  name: string
  status: 'healthy' | 'degraded' | 'down'
  responseTime: number
  lastCheck: Date
}
```

ADMIN PANEL FEATURES:

1. **Dashboard Overview**
   - System health indicators
   - Recent upload activity
   - User activity summary
   - Performance metrics
   - Quick action buttons

2. **Data Management**
   - File upload interface with drag-and-drop
   - Upload progress tracking
   - Data validation results
   - Historical upload logs
   - Data quality metrics

3. **User Administration**
   - User list with search and filtering
   - Role assignment and permissions
   - User activity monitoring
   - Account activation/deactivation
   - Bulk user operations

4. **System Configuration**
   - Application settings
   - Feature flags management
   - Email templates
   - System maintenance mode
   - Backup and restore options

5. **Monitoring & Logs**
   - Real-time system logs
   - Audit trail for all actions
   - Performance monitoring
   - Error tracking and alerts
   - Usage analytics

IMPLEMENTATION COMPONENTS:

1. **Upload Interface**
```jsx
// File upload component
const DataUploadComponent = () => {
  const [uploadState, setUploadState] = useState<UploadState>()
  
  return (
    <div className="upload-container">
      <FileDropzone onFileSelect={handleFileSelect} />
      <UploadProgress progress={uploadState?.progress} />
      <ValidationResults results={uploadState?.validation} />
      <UploadHistory uploads={recentUploads} />
    </div>
  )
}
```

2. **User Management**
```jsx
// User management interface
const UserManagement = () => {
  return (
    <div className="user-management">
      <UserFilters />
      <UserTable users={users} onUserAction={handleUserAction} />
      <UserModal user={selectedUser} onSave={handleUserSave} />
      <BulkActions selectedUsers={selectedUsers} />
    </div>
  )
}
```

3. **System Monitoring**
```jsx
// System health dashboard
const SystemDashboard = () => {
  return (
    <div className="system-dashboard">
      <HealthIndicators health={systemHealth} />
      <PerformanceCharts metrics={performanceMetrics} />
      <ServiceStatus services={services} />
      <RecentActivity activities={recentActivities} />
    </div>
  )
}
```

SECURITY CONSIDERATIONS:

1. **Access Control**
   - Role-based route protection
   - API endpoint authorization
   - File upload security scanning
   - Session management
   - Audit logging for all actions

2. **Data Protection**
   - File type validation
   - Size limits and quotas
   - Virus scanning integration
   - Data encryption at rest
   - Secure file storage

DELIVERABLES:

1. **Complete Admin Panel**
   - All admin pages and components
   - File upload system with validation
   - User management interface
   - System monitoring dashboard

2. **Security Implementation**
   - Role-based access control
   - Secure file handling
   - Audit logging system
   - Session management

3. **Integration Components**
   - API client for backend services
   - Real-time updates with WebSocket
   - Error handling and notifications
   - Performance optimization

4. **Documentation**
   - Admin user guide
   - Security procedures
   - Troubleshooting guide
   - API documentation

SUCCESS CRITERIA:
- Secure file upload with validation
- Real-time progress tracking
- Comprehensive user management
- System health monitoring
- Audit trail for compliance
```

---

## 📁 PHASE 4: INFRASTRUCTURE & DEVOPS

### 🔧 PROMPT 4.1: Containerization & Orchestration

**Role**: Senior DevOps Engineer + Infrastructure Architect
**Technique**: Infrastructure as Code + Best Practices
**Target**: Claude 3.5 Sonnet
**Output**: Complete containerization and orchestration setup

```
ROLE: You are a Senior DevOps Engineer with expertise in containerization, Kubernetes orchestration, and cloud-native infrastructure design.

INFRASTRUCTURE REQUIREMENTS:
Design a scalable, production-ready infrastructure:
- Multi-service containerization with Docker
- Kubernetes orchestration for auto-scaling
- Service mesh for secure communication
- Monitoring and observability stack
- CI/CD pipeline with automated testing
- Multi-environment deployment (dev, staging, prod)

CONTAINERIZATION STRATEGY:

1. **Multi-Stage Docker Builds**
```dockerfile
# Frontend Dockerfile (Next.js)
FROM node:18-alpine AS base
FROM base AS deps
FROM base AS builder
FROM base AS runner

# Backend Dockerfile (Node.js)
FROM node:18-alpine AS base
FROM base AS deps
FROM base AS builder
FROM base AS runner

# Optimization techniques:
# - Multi-stage builds for smaller images
# - Layer caching optimization
# - Security scanning integration
# - Non-root user execution
```

2. **Service Architecture**
```yaml
# Services to containerize:
services:
  - api-gateway
  - auth-service
  - data-ingestion-service
  - analysis-engine-service
  - notification-service
  - admin-service
  - frontend
  - postgres
  - redis
  - rabbitmq
  - minio
```

KUBERNETES ORCHESTRATION:

1. **Cluster Architecture**
```yaml
# Namespace organization
namespaces:
  - seasonality-prod
  - seasonality-staging
  - seasonality-dev
  - monitoring
  - ingress-system

# Resource allocation
resources:
  - CPU limits and requests
  - Memory limits and requests
  - Storage requirements
  - Network policies
```

2. **Deployment Patterns**
```yaml
# Deployment strategies
strategies:
  - Rolling updates for zero-downtime
  - Blue-green deployment for critical services
  - Canary deployment for gradual rollouts
  - A/B testing infrastructure

# Auto-scaling configuration
autoscaling:
  - Horizontal Pod Autoscaler (HPA)
  - Vertical Pod Autoscaler (VPA)
  - Cluster Autoscaler
  - Custom metrics scaling
```

3. **Service Mesh Integration**
```yaml
# Istio service mesh
istio:
  - Traffic management
  - Security policies
  - Observability
  - Circuit breaker patterns
  - Retry and timeout policies
```

MONITORING & OBSERVABILITY:

1. **Prometheus Stack**
```yaml
# Monitoring components
monitoring:
  - Prometheus for metrics collection
  - Grafana for visualization
  - AlertManager for alerting
  - Jaeger for distributed tracing
  - ELK stack for logging
```

2. **Custom Metrics**
```typescript
// Application metrics
interface ApplicationMetrics {
  requestDuration: Histogram
  requestCount: Counter
  activeUsers: Gauge
  analysisProcessingTime: Histogram
  dataIngestionRate: Counter
  errorRate: Counter
}
```

CI/CD PIPELINE:

1. **GitLab CI/CD Configuration**
```yaml
# Pipeline stages
stages:
  - test
  - build
  - security-scan
  - deploy-dev
  - integration-test
  - deploy-staging
  - e2e-test
  - deploy-prod

# Automated testing
testing:
  - Unit tests with coverage
  - Integration tests
  - Security scanning
  - Performance testing
  - E2E testing with Playwright
```

2. **Deployment Automation**
```yaml
# Helm charts for deployment
helm:
  - Chart templates for all services
  - Environment-specific values
  - Secret management with Sealed Secrets
  - ConfigMap management
  - Ingress configuration
```

DELIVERABLES:

1. **Docker Configuration**
   - Optimized Dockerfiles for all services
   - Docker Compose for local development
   - Multi-arch build support
   - Security scanning integration

2. **Kubernetes Manifests**
   - Complete Helm charts
   - Namespace and RBAC configuration
   - Service mesh setup
   - Ingress and networking

3. **Monitoring Setup**
   - Prometheus configuration
   - Grafana dashboards
   - Alert rules and notifications
   - Logging aggregation

4. **CI/CD Pipeline**
   - GitLab CI configuration
   - Automated testing pipeline
   - Security scanning integration
   - Deployment automation

5. **Documentation**
   - Infrastructure architecture
   - Deployment procedures
   - Troubleshooting guides
   - Disaster recovery plans

PERFORMANCE TARGETS:
- Container startup time: <30 seconds
- Auto-scaling response: <2 minutes
- Deployment time: <10 minutes
- System availability: 99.9%
- Resource utilization: 70-80%
```

### 🔧 PROMPT 4.2: Security & Compliance Framework

**Role**: Security Engineer + Compliance Specialist
**Technique**: Security-by-Design + Compliance Framework
**Target**: Claude 3.5 Sonnet
**Output**: Complete security and compliance implementation

```
ROLE: You are a Security Engineer with expertise in application security, infrastructure security, and compliance frameworks (SOC2, GDPR, PCI-DSS).

SECURITY FRAMEWORK REQUIREMENTS:
Implement enterprise-grade security for financial SaaS:
- Zero-trust security model
- Data encryption at rest and in transit
- Comprehensive audit logging
- Vulnerability management
- Incident response procedures
- Compliance with financial regulations

SECURITY ARCHITECTURE:

1. **Zero-Trust Implementation**
```yaml
# Network security
network_security:
  - Network segmentation with policies
  - Mutual TLS (mTLS) for service communication
  - API Gateway with authentication
  - VPN access for administrators
  - Firewall rules and monitoring

# Identity and access management
iam:
  - Multi-factor authentication (MFA)
  - Role-based access control (RBAC)
  - Principle of least privilege
  - Regular access reviews
  - Automated deprovisioning
```

2. **Data Protection**
```typescript
// Encryption standards
interface EncryptionConfig {
  atRest: {
    algorithm: 'AES-256-GCM'
    keyRotation: '90 days'
    keyManagement: 'AWS KMS' | 'HashiCorp Vault'
  }
  inTransit: {
    protocol: 'TLS 1.3'
    certificateManagement: 'Let\'s Encrypt' | 'Internal CA'
    hsts: true
  }
  application: {
    sensitiveFields: string[]
    tokenization: boolean
    fieldLevelEncryption: boolean
  }
}
```

3. **Audit & Compliance**
```typescript
// Audit logging
interface AuditLog {
  eventId: string
  timestamp: Date
  userId: string
  tenantId: string
  action: string
  resource: string
  outcome: 'success' | 'failure'
  ipAddress: string
  userAgent: string
  metadata: Record<string, any>
}

// Compliance requirements
interface ComplianceFramework {
  soc2: SOC2Requirements
  gdpr: GDPRRequirements
  iso27001: ISO27001Requirements
  pciDss?: PCIDSSRequirements
}
```

SECURITY CONTROLS:

1. **Application Security**
   - Input validation and sanitization
   - SQL injection prevention
   - XSS protection
   - CSRF protection
   - Secure session management
   - API rate limiting
   - File upload security

2. **Infrastructure Security**
   - Container security scanning
   - Vulnerability management
   - Patch management
   - Network monitoring
   - Intrusion detection
   - Security incident response

3. **Data Security**
   - Data classification
   - Data loss prevention (DLP)
   - Backup encryption
   - Secure data disposal
   - Data retention policies
   - Privacy controls

COMPLIANCE IMPLEMENTATION:

1. **SOC2 Type II**
```yaml
# Security controls
soc2_controls:
  - Access controls and authentication
  - System monitoring and logging
  - Change management procedures
  - Data backup and recovery
  - Incident response procedures
  - Vendor management
  - Risk assessment processes
```

2. **GDPR Compliance**
```typescript
// Privacy controls
interface GDPRControls {
  dataMinimization: boolean
  consentManagement: ConsentFramework
  rightToErasure: DataDeletionProcess
  dataPortability: DataExportProcess
  privacyByDesign: boolean
  dataProtectionOfficer: ContactInfo
}
```

3. **Financial Regulations**
   - Data residency requirements
   - Financial data protection
   - Audit trail requirements
   - Regulatory reporting
   - Customer data protection

SECURITY MONITORING:

1. **SIEM Integration**
```yaml
# Security monitoring
siem:
  - Log aggregation and analysis
  - Threat detection rules
  - Automated incident response
  - Forensic capabilities
  - Compliance reporting
```

2. **Vulnerability Management**
```yaml
# Vulnerability scanning
vulnerability_management:
  - Automated security scanning
  - Dependency vulnerability checks
  - Container image scanning
  - Infrastructure scanning
  - Penetration testing schedule
```

DELIVERABLES:

1. **Security Architecture**
   - Zero-trust implementation
   - Encryption configuration
   - Access control framework
   - Network security design

2. **Compliance Framework**
   - SOC2 control implementation
   - GDPR compliance measures
   - Audit logging system
   - Policy documentation

3. **Security Monitoring**
   - SIEM configuration
   - Vulnerability management
   - Incident response procedures
   - Security metrics dashboard

4. **Documentation**
   - Security policies and procedures
   - Compliance documentation
   - Incident response playbooks
   - Security training materials

SECURITY METRICS:
- Mean time to detect (MTTD): <15 minutes
- Mean time to respond (MTTR): <1 hour
- Vulnerability remediation: <48 hours for critical
- Security training completion: 100%
- Compliance audit score: >95%
```

---

## 📁 PHASE 5: ADVANCED FEATURES & OPTIMIZATION

### 🔧 PROMPT 5.1: Real-time Analytics & Notifications

**Role**: Senior Software Engineer + Real-time Systems Architect
**Technique**: Event-Driven Architecture + Performance Optimization
**Target**: Claude 3.5 Sonnet
**Output**: Real-time analytics and notification system

```
ROLE: You are a Senior Software Engineer specializing in real-time systems, event-driven architecture, and high-performance analytics platforms.

REAL-TIME REQUIREMENTS:
Build advanced real-time capabilities beyond the old Python system:
- Live data processing and analysis
- Real-time notifications and alerts
- WebSocket-based updates
- Event-driven architecture
- Stream processing capabilities
- Performance monitoring and optimization

REAL-TIME ARCHITECTURE:

1. **Event Streaming Platform**
```typescript
// Event streaming with Apache Kafka/RabbitMQ
interface EventStream {
  topic: string
  partition?: number
  key: string
  value: any
  timestamp: Date
  headers?: Record<string, string>
}

// Event types for real-time processing
enum RealTimeEvent {
  DATA_UPDATED = 'data.updated',
  ANALYSIS_COMPLETED = 'analysis.completed',
  ALERT_TRIGGERED = 'alert.triggered',
  USER_ACTION = 'user.action',
  SYSTEM_HEALTH = 'system.health'
}
```

2. **Stream Processing Engine**
```typescript
// Stream processing for real-time analytics
class StreamProcessor {
  async processDataStream(stream: EventStream): Promise<ProcessedData>
  async calculateRealTimeMetrics(data: StreamData): Promise<Metrics>
  async detectAnomalies(data: TimeSeriesData): Promise<Anomaly[]>
  async triggerAlerts(conditions: AlertCondition[]): Promise<Alert[]>
}

// Real-time aggregations
interface RealTimeAggregation {
  windowType: 'tumbling' | 'sliding' | 'session'
  windowSize: Duration
  aggregationType: 'sum' | 'avg' | 'count' | 'min' | 'max'
  groupBy: string[]
}
```

3. **WebSocket Management**
```typescript
// WebSocket server for real-time updates
class WebSocketManager {
  private connections: Map<string, WebSocket>
  
  async broadcastToTenant(tenantId: string, event: RealTimeEvent): Promise<void>
  async sendToUser(userId: string, message: any): Promise<void>
  async subscribeToChannel(userId: string, channel: string): Promise<void>
  async handleConnection(socket: WebSocket, userId: string): Promise<void>
}

// Real-time subscriptions
interface Subscription {
  userId: string
  tenantId: string
  channels: string[]
  filters: SubscriptionFilter[]
  isActive: boolean
}
```

NOTIFICATION SYSTEM:

1. **Multi-Channel Notifications**
```typescript
// Notification channels
enum NotificationChannel {
  IN_APP = 'in_app',
  EMAIL = 'email',
  SMS = 'sms',
  WEBHOOK = 'webhook',
  SLACK = 'slack',
  TEAMS = 'teams'
}

// Notification templates
interface NotificationTemplate {
  id: string
  name: string
  channel: NotificationChannel
  template: string
  variables: string[]
  isActive: boolean
}

// Alert configuration
interface AlertRule {
  id: string
  name: string
  condition: AlertCondition
  channels: NotificationChannel[]
  recipients: string[]
  cooldownPeriod: Duration
  isActive: boolean
}
```

2. **Smart Alerting**
```typescript
// Intelligent alert system
class SmartAlertEngine {
  async evaluateConditions(data: any[], rules: AlertRule[]): Promise<Alert[]>
  async preventAlertFatigue(alerts: Alert[]): Promise<Alert[]>
  async escalateAlerts(alert: Alert): Promise<void>
  async trackAlertMetrics(): Promise<AlertMetrics>
}

// Alert conditions
interface AlertCondition {
  metric: string
  operator: 'gt' | 'lt' | 'eq' | 'ne' | 'between'
  threshold: number | [number, number]
  timeWindow: Duration
  aggregation?: 'avg' | 'sum' | 'count' | 'min' | 'max'
}
```

PERFORMANCE OPTIMIZATION:

1. **Caching Strategy**
```typescript
// Multi-level caching
interface CacheStrategy {
  l1Cache: MemoryCache    // In-memory for hot data
  l2Cache: RedisCache     // Redis for shared cache
  l3Cache: DatabaseCache  // Database query cache
  cdnCache: CDNCache      // CDN for static assets
}

// Cache invalidation
class CacheManager {
  async invalidateByPattern(pattern: string): Promise<void>
  async warmCache(keys: string[]): Promise<void>
  async getCacheStats(): Promise<CacheStats>
  async optimizeCacheUsage(): Promise<void>
}
```

2. **Database Optimization**
```typescript
// Query optimization
interface QueryOptimization {
  indexStrategy: IndexStrategy[]
  partitioning: PartitionStrategy
  connectionPooling: PoolConfig
  readReplicas: ReplicaConfig[]
}

// Performance monitoring
class PerformanceMonitor {
  async trackQueryPerformance(): Promise<QueryMetrics>
  async identifySlowQueries(): Promise<SlowQuery[]>
  async optimizeIndexes(): Promise<IndexRecommendation[]>
  async monitorResourceUsage(): Promise<ResourceMetrics>
}
```

DELIVERABLES:

1. **Real-time Processing Engine**
   - Event streaming implementation
   - Stream processing capabilities
   - Real-time analytics engine
   - WebSocket server and client

2. **Notification System**
   - Multi-channel notification service
   - Alert rule engine
   - Template management system
   - Delivery tracking and metrics

3. **Performance Optimization**
   - Multi-level caching system
   - Database optimization
   - Query performance monitoring
   - Resource usage optimization

4. **Monitoring & Metrics**
   - Real-time performance dashboard
   - Alert effectiveness metrics
   - System health monitoring
   - User engagement analytics

PERFORMANCE TARGETS:
- Real-time event processing: <100ms latency
- WebSocket message delivery: <50ms
- Cache hit ratio: >90%
- Alert delivery time: <30 seconds
- System throughput: 10,000+ events/second
```

### 🔧 PROMPT 5.2: Advanced Analytics & Machine Learning

**Role**: Senior Data Scientist + ML Engineer
**Technique**: ML Pipeline Design + Advanced Analytics
**Target**: Claude 3.5 Sonnet
**Output**: ML-powered analytics and prediction system

```
ROLE: You are a Senior Data Scientist with expertise in financial machine learning, time-series analysis, and production ML systems.

ML ENHANCEMENT VISION:
Extend beyond the old Python system with AI-powered features:
- Predictive analytics for market trends
- Anomaly detection in trading patterns
- Automated pattern recognition
- Intelligent recommendations
- Risk assessment models
- Sentiment analysis integration

MACHINE LEARNING ARCHITECTURE:

1. **ML Pipeline Framework**
```typescript
// ML model lifecycle management
interface MLPipeline {
  dataIngestion: DataIngestionStage
  featureEngineering: FeatureEngineeringStage
  modelTraining: ModelTrainingStage
  modelValidation: ModelValidationStage
  modelDeployment: ModelDeploymentStage
  modelMonitoring: ModelMonitoringStage
}

// Feature engineering for financial data
class FeatureEngineer {
  async calculateTechnicalIndicators(data: PriceData[]): Promise<TechnicalIndicators>
  async generateSeasonalFeatures(data: TimeSeriesData): Promise<SeasonalFeatures>
  async createLaggedFeatures(data: any[], lags: number[]): Promise<LaggedFeatures>
  async normalizeFeatures(features: Features): Promise<NormalizedFeatures>
}
```

2. **Predictive Models**
```typescript
// Time-series forecasting
interface ForecastingModel {
  modelType: 'ARIMA' | 'LSTM' | 'Prophet' | 'Transformer'
  hyperparameters: Record<string, any>
  trainingData: TimeSeriesData[]
  validationMetrics: ValidationMetrics
  predictionHorizon: number
}

// Pattern recognition
class PatternRecognition {
  async detectSeasonalPatterns(data: TimeSeriesData): Promise<SeasonalPattern[]>
  async identifyTrendReversals(data: PriceData[]): Promise<TrendReversal[]>
  async findSimilarPatterns(pattern: Pattern, data: TimeSeriesData): Promise<SimilarPattern[]>
  async classifyMarketRegimes(data: MarketData): Promise<MarketRegime>
}
```

3. **Anomaly Detection**
```typescript
// Anomaly detection system
class AnomalyDetector {
  async detectPriceAnomalies(data: PriceData[]): Promise<PriceAnomaly[]>
  async identifyVolumeSpikes(data: VolumeData[]): Promise<VolumeAnomaly[]>
  async findUnusualPatterns(data: PatternData): Promise<PatternAnomaly[]>
  async assessRiskLevels(anomalies: Anomaly[]): Promise<RiskAssessment>
}

// Risk assessment models
interface RiskModel {
  modelName: string
  riskFactors: RiskFactor[]
  riskScore: number
  confidence: number
  recommendations: Recommendation[]
}
```

ADVANCED ANALYTICS:

1. **Intelligent Recommendations**
```typescript
// Recommendation engine
class RecommendationEngine {
  async recommendOptimalTiming(symbol: string, strategy: TradingStrategy): Promise<TimingRecommendation>
  async suggestSimilarSymbols(symbol: string): Promise<SimilarSymbol[]>
  async recommendPortfolioOptimization(portfolio: Portfolio): Promise<OptimizationSuggestion>
  async generateInsights(analysisResults: AnalysisResult[]): Promise<Insight[]>
}

// Market sentiment analysis
interface SentimentAnalysis {
  newsAnalysis: NewsSentiment
  socialMediaSentiment: SocialSentiment
  marketSentiment: MarketSentiment
  overallSentiment: SentimentScore
}
```

2. **Advanced Backtesting**
```typescript
// Enhanced backtesting engine
class BacktestingEngine {
  async runMonteCarloSimulation(strategy: Strategy, iterations: number): Promise<SimulationResult>
  async performWalkForwardAnalysis(strategy: Strategy): Promise<WalkForwardResult>
  async calculateRiskMetrics(returns: number[]): Promise<RiskMetrics>
  async optimizeParameters(strategy: Strategy): Promise<OptimizedStrategy>
}

// Performance attribution
interface PerformanceAttribution {
  factorReturns: FactorReturn[]
  specificReturns: SpecificReturn[]
  interactionEffects: InteractionEffect[]
  totalAttribution: Attribution
}
```

ML INFRASTRUCTURE:

1. **Model Serving**
```typescript
// Model deployment and serving
class ModelServer {
  async loadModel(modelId: string): Promise<MLModel>
  async predict(model: MLModel, features: Features): Promise<Prediction>
  async batchPredict(model: MLModel, batch: Features[]): Promise<Prediction[]>
  async updateModel(modelId: string, newModel: MLModel): Promise<void>
}

// A/B testing for models
interface ModelExperiment {
  experimentId: string
  controlModel: MLModel
  treatmentModel: MLModel
  trafficSplit: number
  metrics: ExperimentMetrics
  status: ExperimentStatus
}
```

2. **Model Monitoring**
```typescript
// Model performance monitoring
class ModelMonitor {
  async trackModelPerformance(modelId: string): Promise<PerformanceMetrics>
  async detectModelDrift(model: MLModel, newData: any[]): Promise<DriftDetection>
  async monitorDataQuality(data: any[]): Promise<DataQualityReport>
  async alertOnPerformanceDegradation(threshold: number): Promise<Alert[]>
}

// Explainable AI
interface ModelExplanation {
  featureImportance: FeatureImportance[]
  shapeValues: ShapeValue[]
  localExplanations: LocalExplanation[]
  globalExplanations: GlobalExplanation[]
}
```

DELIVERABLES:

1. **ML Pipeline Infrastructure**
   - Complete ML pipeline framework
   - Feature engineering system
   - Model training and validation
   - Model deployment and serving

2. **Predictive Analytics**
   - Time-series forecasting models
   - Pattern recognition system
   - Anomaly detection engine
   - Risk assessment models

3. **Advanced Analytics**
   - Recommendation engine
   - Sentiment analysis integration
   - Enhanced backtesting system
   - Performance attribution analysis

4. **ML Operations**
   - Model monitoring and alerting
   - A/B testing framework
   - Explainable AI components
   - Model lifecycle management

ML PERFORMANCE TARGETS:
- Model inference time: <100ms
- Prediction accuracy: >85% for trend direction
- Anomaly detection precision: >90%
- Model training time: <2 hours
- Feature engineering latency: <10ms
```

---

## 📋 COMPREHENSIVE TODO CHECKLIST

### ✅ PHASE 1: FOUNDATION & ARCHITECTURE (Week 1)
**Plan → Research → Execute**

#### 🔍 Research Phase
- [ ] **PROMPT 1.1**: Analyze old software architecture and requirements
- [ ] **PROMPT 1.2**: Research modern SaaS architecture patterns
- [ ] Study microservices best practices and patterns
- [ ] Research cloud-native technologies and tools
- [ ] Analyze scalability and performance requirements

#### 🏗️ Execute Phase
- [ ] **PROMPT 1.1**: Complete system architecture design
- [ ] **PROMPT 1.2**: Design comprehensive database schema
- [ ] Create technology stack documentation
- [ ] Set up development environment and tools
- [ ] Establish coding standards and guidelines

### ✅ PHASE 2: BACKEND MICROSERVICES (Weeks 2-3)
**Plan → Research → Execute**

#### 🔍 Research Phase
- [ ] **PROMPT 2.1**: Research API Gateway patterns and security
- [ ] **PROMPT 2.2**: Study event-driven architecture patterns
- [ ] **PROMPT 2.3**: Analyze financial calculation algorithms
- [ ] Research microservices communication patterns
- [ ] Study data processing and ETL best practices

#### 🏗️ Execute Phase
- [ ] **PROMPT 2.1**: Build API Gateway and Authentication Service
- [ ] **PROMPT 2.2**: Implement Data Ingestion & Processing Service
- [ ] **PROMPT 2.3**: Develop Analysis Engine Service
- [ ] Create additional microservices (Notification, Admin)
- [ ] Implement service-to-service communication
- [ ] Set up message queues and event streaming

### ✅ PHASE 3: FRONTEND FOUNDATION (Week 3-4)
**Plan → Research → Execute**

#### 🔍 Research Phase
- [ ] **PROMPT 3.1**: Research Next.js 14 best practices
- [ ] **PROMPT 3.2**: Study admin panel design patterns
- [ ] Research modern React patterns and state management
- [ ] Analyze UI/UX requirements for financial applications
- [ ] Study accessibility and performance optimization

#### 🏗️ Execute Phase
- [ ] **PROMPT 3.1**: Build Next.js application foundation
- [ ] **PROMPT 3.2**: Create comprehensive admin panel
- [ ] Implement authentication and routing
- [ ] Build basic analysis page templates
- [ ] Set up state management and API integration
- [ ] **Note**: Keep UI simple for AI Studio enhancement later

### ✅ PHASE 4: INFRASTRUCTURE & DEVOPS (Week 4-5)
**Plan → Research → Execute**

#### 🔍 Research Phase
- [ ] **PROMPT 4.1**: Research containerization best practices
- [ ] **PROMPT 4.2**: Study security and compliance frameworks
- [ ] Research Kubernetes orchestration patterns
- [ ] Study CI/CD pipeline design
- [ ] Analyze monitoring and observability tools

#### 🏗️ Execute Phase
- [ ] **PROMPT 4.1**: Implement containerization and orchestration
- [ ] **PROMPT 4.2**: Build security and compliance framework
- [ ] Set up CI/CD pipeline with automated testing
- [ ] Implement monitoring and logging stack
- [ ] Configure production deployment environment
- [ ] Set up backup and disaster recovery procedures

### ✅ PHASE 5: ADVANCED FEATURES (Week 5-6)
**Plan → Research → Execute**

#### 🔍 Research Phase
- [ ] **PROMPT 5.1**: Research real-time systems architecture
- [ ] **PROMPT 5.2**: Study ML pipeline design patterns
- [ ] Research WebSocket implementation patterns
- [ ] Study financial ML algorithms and models
- [ ] Analyze performance optimization techniques

#### 🏗️ Execute Phase
- [ ] **PROMPT 5.1**: Build real-time analytics and notifications
- [ ] **PROMPT 5.2**: Implement ML-powered analytics
- [ ] Set up WebSocket infrastructure
- [ ] Implement caching and performance optimization
- [ ] Build advanced analytics dashboard
- [ ] Create ML model training and deployment pipeline

### ✅ PHASE 6: INTEGRATION & TESTING (Week 6-7)
**Plan → Research → Execute**

#### 🔍 Research Phase
- [ ] Research comprehensive testing strategies
- [ ] Study performance testing methodologies
- [ ] Analyze security testing approaches
- [ ] Research user acceptance testing patterns
- [ ] Study load testing and scalability validation

#### 🏗️ Execute Phase
- [ ] Implement comprehensive test suite (unit, integration, E2E)
- [ ] Perform security testing and vulnerability assessment
- [ ] Conduct performance and load testing
- [ ] Execute user acceptance testing
- [ ] Validate all algorithms against old Python system
- [ ] Optimize performance based on test results

### ✅ PHASE 7: DEPLOYMENT & LAUNCH (Week 7-8)
**Plan → Research → Execute**

#### 🔍 Research Phase
- [ ] Research production deployment strategies
- [ ] Study user migration best practices
- [ ] Analyze monitoring and alerting requirements
- [ ] Research documentation and training approaches
- [ ] Study post-launch support procedures

#### 🏗️ Execute Phase
- [ ] Deploy to production environment
- [ ] Migrate data from old system
- [ ] Create user documentation and training materials
- [ ] Set up production monitoring and alerting
- [ ] Conduct user training and onboarding
- [ ] Execute go-live procedures
- [ ] Monitor system performance and user adoption

---

## 🎯 SUCCESS METRICS & KPIs

### Technical Performance
- **API Response Time**: <200ms for 95% of requests
- **System Uptime**: 99.9% availability
- **Scalability**: Support 1,000+ concurrent users
- **Data Processing**: Handle 100MB files in <30 seconds
- **Real-time Updates**: <100ms latency for live data

### Business Metrics
- **User Adoption**: 90% migration from old system within 30 days
- **Feature Utilization**: 80% of features actively used
- **User Satisfaction**: 4.5/5 rating or higher
- **Support Tickets**: <5% of user base requiring support
- **Performance Improvement**: 10x faster than old Python system

### Security & Compliance
- **Security Vulnerabilities**: Zero critical vulnerabilities
- **Compliance Score**: >95% for SOC2/GDPR requirements
- **Data Integrity**: 100% data accuracy during migration
- **Audit Trail**: Complete audit logs for all actions
- **Access Control**: Proper RBAC implementation

---

## 📝 ROLE-SPECIFIC NOTES

### For Software Engineers (SWE)
- Focus on clean, maintainable code architecture
- Implement proper error handling and logging
- Follow SOLID principles and design patterns
- Write comprehensive unit and integration tests
- Document APIs and code thoroughly

### For Software Development Engineers (SDE)
- Design scalable and performant systems
- Implement proper caching and optimization
- Focus on algorithm efficiency and accuracy
- Build robust data processing pipelines
- Ensure system reliability and fault tolerance

### For Backend Developers
- Design RESTful APIs with proper versioning
- Implement secure authentication and authorization
- Build efficient database queries and operations
- Handle concurrent requests and race conditions
- Implement proper monitoring and health checks

### For Infrastructure Developers
- Design cloud-native, scalable infrastructure
- Implement Infrastructure as Code (IaC)
- Set up comprehensive monitoring and alerting
- Ensure security and compliance requirements
- Plan for disaster recovery and backup procedures

---

## 🚀 AI STUDIO INTEGRATION NOTES

### Frontend Enhancement Strategy
The frontend built in Phase 3 will be intentionally simple and functional, designed for easy enhancement with AI Studio:

1. **Clean Component Architecture**: Well-structured React components that can be easily styled
2. **Proper Data Flow**: State management and API integration ready for UI enhancements
3. **Accessibility Foundation**: Semantic HTML and ARIA labels for AI Studio to build upon
4. **Responsive Framework**: Basic responsive design that can be enhanced with advanced layouts

### AI Studio Enhancement Areas
- **Advanced UI/UX Design**: Transform simple components into beautiful, modern interfaces
- **Interactive Visualizations**: Enhance basic charts with advanced Plotly.js/D3.js visualizations
- **Complex Animations**: Add smooth transitions and micro-interactions
- **Advanced Filtering**: Create sophisticated filter interfaces with real-time updates
- **Dashboard Layouts**: Design professional financial dashboard layouts

### Handoff Documentation
Create detailed documentation for AI Studio enhancement:
- Component structure and props
- Data flow and state management
- API endpoints and data formats
- Styling guidelines and design tokens
- Performance considerations and optimization opportunities

---

*This comprehensive prompt guide provides a complete roadmap for building a modern, scalable Seasonality SaaS from scratch. Each prompt is designed with specific roles, techniques, and deliverables to ensure successful implementation by AI agents and human developers.*