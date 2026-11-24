using System;
using System.Collections.Generic;
using System.Linq;
using SPS.Abstractions;

namespace SPS.Student;

/// <summary>
/// Адаптивная стратегия для Service Profit Simulator.
/// Все решения основаны только на данных, доступных через SPS.Abstractions.
/// </summary>
public sealed class AdvancedSystemPolicy2 : ISystemPolicy
{
    public IRequestPolicy RpcPolicy { get; } = new AdaptiveRequestPolicy();

    private const double ApiLoadWeight = 1.0;

    // Сохраняем увеличенную мощность после всплеска нагрузки, чтобы не запускать
    // и останавливать платные экземпляры каждую секунду.
    private int workerTarget = 4;
    private TimeSpan? lowLoadSince;

    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        // Не ограничиваем Generate слишком коротким бюджетом:
        // в открытом сценарии терпение клиента существенно больше времени расчёта.
        return request.Kind switch
        {
            RequestKind.Metadata => TimeSpan.FromSeconds(2.0),
            RequestKind.Preview => TimeSpan.FromSeconds(5.0),
            RequestKind.Generate => TimeSpan.FromSeconds(45.0),
            _ => null
        };
    }

    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        var candidates = context.Endpoints
            .Where(e => e.Role == InstanceRole.Api &&
                        e.State == InstanceState.Ready &&
                        e.Mode == EndpointMode.AllowTraffic)
            .ToList();

        if (candidates.Count == 0)
            return null; // Ждём следующего ревью, а не отклоняем платный запрос.

        // Оцениваем относительную загрузку с учётом числа слотов и скорости.
        // Небольшой штраф за занятые запросы помогает избегать перегруженных API.
        var selected = candidates
            .OrderBy(e =>
            {
                double capacity = Math.Max(1.0, e.Slots * Math.Max(0.1, e.Speed));
                double load = e.OutstandingCalls / capacity;
                return load * ApiLoadWeight + (e.OutstandingCalls * 0.015);
            })
            .ThenBy(e => e.Id)
            .First();

        return EndpointSelectionDecision.Assign(selected.Id);
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        var endpoint = context.Endpoint;

        if (endpoint.State == InstanceState.Draining)
            return EndpointDecision.Block();

        // Для воркеров решение о восстановлении принимается на уровне заданий
        // по heartbeat. Не блокируем весь воркер из-за одного долгого задания.
        if (endpoint.Role == InstanceRole.Worker)
            return EndpointDecision.Allow();

        var recent = context.RecentResults
            .Where(r => !r.IsProbe)
            .OrderByDescending(r => r.Time)
            .Take(5)
            .ToList();

        var lastProbe = context.RecentResults
            .Where(r => r.IsProbe)
            .OrderByDescending(r => r.Time)
            .FirstOrDefault();

        if (lastProbe != null && lastProbe.Outcome == RequestOutcome.Success)
            return EndpointDecision.Allow();

        // После неудачной проверки повторяем probe, не открывая обычный трафик.
        if (lastProbe != null && lastProbe.Outcome != RequestOutcome.Success)
            return EndpointDecision.Probe();

        if (recent.Count >= 3)
        {
            int failures = recent.Count(r => r.Outcome != RequestOutcome.Success);
            int consecutiveFailures = 0;

            foreach (var result in recent)
            {
                if (result.Outcome == RequestOutcome.Success)
                    break;
                consecutiveFailures++;
            }

            // Короткая серия ошибок — повод проверить сервер отдельно.
            if (consecutiveFailures >= 2 || failures >= 3)
                return EndpointDecision.Probe();
        }

        return EndpointDecision.Allow();
    }

    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        var entry = context.Entry;

        // TTL ещё не истёк, но запись может быть слишком старой для конкретного клиента.
        if (entry != null &&
            context.Now - entry.SourceObservedAt <= context.Request.MaxDataAge)
        {
            return CacheReadDecision.ServeCached();
        }

        // Объединяем одинаковые параллельные загрузки: один поход к источнику
        // обслужит всех ожидающих клиентов. Свежесть проверит движок отдельно
        // для каждого клиента при доставке результата.
        var pending = context.PendingFetches
            .Where(f => f.Key == context.Request.ResourceKey)
            .OrderBy(f => f.StartedAt)
            .FirstOrDefault();

        if (pending != null)
            return CacheReadDecision.JoinFetch(pending.Id);

        return CacheReadDecision.StartFetch();
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        var value = context.Value;

        if (value.SizeBytes <= 0 || value.SizeBytes > context.CapacityBytes)
            return CacheWriteDecision.Skip();

        // При заполнении освобождаем место по принципу приближённого LRU:
        // в первую очередь удаляем наиболее старые по времени получения записи.
        long retainedBytes = context.Entries
            .Where(e => e.Key != value.Key)
            .Sum(e => e.SizeBytes);

        var evictions = new List<string>();

        foreach (var oldEntry in context.Entries
                     .Where(e => e.Key != value.Key)
                     .OrderBy(e => e.ExpiresAt <= context.Now ? 0 : 1)
                     .ThenBy(e => e.SourceObservedAt))
        {
            if (retainedBytes + value.SizeBytes <= context.CapacityBytes)
                break;

            evictions.Add(oldEntry.Key);
            retainedBytes -= oldEntry.SizeBytes;
        }

        if (retainedBytes + value.SizeBytes > context.CapacityBytes)
            return CacheWriteDecision.Skip();

        // Небольшой TTL уменьшает риск повторного использования устаревших данных.
        return CacheWriteDecision.Retain(TimeSpan.FromSeconds(45), evictions.ToArray());
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0)
            return null;

        // Сначала назначаем самые старые задания, чтобы не допускать голодания.
        // При одинаковом времени в очереди выше приоритет у более дорогого Generate.
        var job = context.QueuedJobs
            .OrderByDescending(j => j.Request.Revenue /
                (decimal)Math.Max(0.5, j.Request.EstimatedWorkSeconds))
            .ThenBy(j => j.QueuedAt)
            .ThenBy(j => j.Id)
            .First();

        var worker = context.AvailableWorkers
            .Where(w => w.Role == InstanceRole.Worker &&
                        w.State == InstanceState.Ready &&
                        w.Mode == EndpointMode.AllowTraffic)
            // AvailableWorkers уже содержит только экземпляры со свободным слотом.
            // Поэтому выбираем самый быстрый, а не сортируем одинаковые нулевые очереди.
            .OrderByDescending(w => w.Speed)
            .ThenBy(w => w.Id)
            .FirstOrDefault();

        if (worker == null)
            return null;

        return JobAssignmentDecision.Assign(job.Id, worker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        var job = context.Job;

        if (!job.AssignedAt.HasValue)
            return JobRecoveryDecision.KeepWaiting();

        TimeSpan heartbeatInterval = TimeSpan.FromMilliseconds(500);
        TimeSpan staleHeartbeatLimit = TimeSpan.FromTicks(heartbeatInterval.Ticks * 5);

        // До первого heartbeat даём воркеру время принять задание и начать работу.
        if (!job.LastHeartbeat.HasValue)
        {
            if (context.Now - job.AssignedAt.Value > TimeSpan.FromSeconds(4))
                return JobRecoveryDecision.RequeueAfter(TimeSpan.FromSeconds(1));

            return JobRecoveryDecision.KeepWaiting();
        }

        // Потеря одного heartbeat не означает отказ. Возвращаем задание в очередь
        // только после достаточно длительного отсутствия подтверждений.
        if (context.Now - job.LastHeartbeat.Value > staleHeartbeatLimit)
            return JobRecoveryDecision.RequeueAfter(TimeSpan.FromSeconds(1));

        return JobRecoveryDecision.KeepWaiting();
    }

    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        var settings = context.Settings;

        // API в открытом сценарии не является главным узким местом:
        // два API дают 8 слотов, поэтому не увеличиваем их без явной очереди.
        int desiredApis = Math.Min(2, settings.MaximumApis);

        // Учитываем и задания в очереди, и уже выполняемые задания.
        // Ранее масштабирование опиралось на очередь отдельно, из-за чего
        // при кратковременном опустошении очереди воркеры постоянно выключались.
        int queuedJobs = context.QueuedJobs.Count;
        int assignedJobs = context.Instances
            .Where(i => i.Role == InstanceRole.Worker && i.State != InstanceState.Stopped)
            .Sum(i => i.AssignedJobs);
        int inFlightJobs = queuedJobs + assignedJobs;

        // Быстрое масштабирование вверх при реальном накоплении работы.
        if (inFlightJobs >= 10)
        {
            workerTarget = Math.Min(8, settings.MaximumWorkers);
            lowLoadSince = null;
        }
        else if (inFlightJobs >= 6)
        {
            workerTarget = Math.Max(workerTarget, Math.Min(6, settings.MaximumWorkers));
            lowLoadSince = null;
        }
        else if (inFlightJobs >= 3)
        {
            workerTarget = Math.Max(workerTarget, Math.Min(5, settings.MaximumWorkers));
            lowLoadSince = null;
        }
        else if (inFlightJobs <= 2)
        {
            // Снижаем мощность только после 20 секунд устойчиво низкой нагрузки.
            // Это предотвращает повторные запуски с трёхсекундным startup delay.
            lowLoadSince ??= context.Now;
            if (context.Now - lowLoadSince.Value >= TimeSpan.FromSeconds(20))
                workerTarget = Math.Min(4, settings.MaximumWorkers);
        }
        else
        {
            lowLoadSince = null;
        }

        int desiredWorkers = Math.Clamp(workerTarget, 1, settings.MaximumWorkers);
        desiredApis = Math.Clamp(desiredApis, 1, settings.MaximumApis);

        // Соблюдаем ограничения по compute units. Сначала сокращаем воркеров,
        // поскольку API обслуживают Preview и регистрацию Generate.
        int unitLimit = settings.ComputeUnitLimit;
        int apiUnits = settings.Api.ComputeUnits;
        int workerUnits = settings.Worker.ComputeUnits;

        while (desiredApis * apiUnits + desiredWorkers * workerUnits > unitLimit)
        {
            if (desiredWorkers > 1)
                desiredWorkers--;
            else if (desiredApis > 1)
                desiredApis--;
            else
                break;
        }

        return new CapacityPlan(desiredApis, desiredWorkers);
    }

    public void Observe(SystemObservation observation)
    {
        // Решения принимаются по актуальным контекстам; отдельная история событий
        // для этой версии стратегии не требуется.
    }

    /// <summary>
    /// RPC-политика из Lab1: адаптивный timeout, ограниченные повторы,
    /// экспоненциальный backoff и jitter.
    /// </summary>
    private sealed class AdaptiveRequestPolicy : IRequestPolicy
    {
        private readonly object sync = new();

        private const int DefaultMaxAttempts = 3;
        private const double MinTimeoutMs = 200.0;
        private const double MaxTimeoutMs = 6000.0;

        private double emaDurationMs = 350.0;
        private double emaDeviationMs = 100.0;
        private int consecutiveTimeouts;

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            lock (sync)
            {
                double timeout = emaDurationMs + 2.2 * emaDeviationMs;

                if (consecutiveTimeouts > 2)
                    timeout *= 1.5;

                return TimeSpan.FromMilliseconds(
                    Math.Clamp(timeout, MinTimeoutMs, MaxTimeoutMs));
            }
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            lock (sync)
            {
                bool success = observation.Outcome == RequestOutcome.Success;

                if (success)
                {
                    consecutiveTimeouts = Math.Max(0, consecutiveTimeouts - 1);

                    double duration = observation.Elapsed.TotalMilliseconds;
                    double delta = Math.Abs(duration - emaDurationMs);

                    emaDurationMs = 0.3 * duration + 0.7 * emaDurationMs;
                    emaDeviationMs = 0.3 * delta + 0.7 * emaDeviationMs;

                    return RetryDecision.NoRetry();
                }

                bool isTimeout = observation.Outcome == RequestOutcome.Timeout;

                if (isTimeout)
                    consecutiveTimeouts++;

                int maxAttempts = DefaultMaxAttempts;

                if (consecutiveTimeouts > 4)
                    maxAttempts = 1;
                else if (consecutiveTimeouts > 2)
                    maxAttempts = 2;

                if (observation.Attempt >= maxAttempts)
                    return RetryDecision.NoRetry();

                double delayMs = isTimeout ? 500.0 : 200.0;
                delayMs *= Math.Pow(2.5, Math.Max(0, observation.Attempt - 1));

                if (consecutiveTimeouts > 3)
                    delayMs *= 2.0;

                double jitter = 0.5 + random.NextDouble() * 1.5;
                delayMs = Math.Clamp(delayMs * jitter, 100.0, 5000.0);

                return RetryDecision.RetryAfter(TimeSpan.FromMilliseconds(delayMs));
            }
        }
    }
}
